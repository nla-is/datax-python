import pickle
import queue
import threading

import grpc
import json
import os
import msgpack
from .protocol.datax_sdk_protocol_pb2 import NextOptions, EmitMessage, Request, GetRequestOptions, Reply
from .protocol.datax_sdk_protocol_pb2_grpc import DataXStub


class FanOutServer(object):
    class DataWithIndex:
        def __init__(self, index, data):
            self.index = index
            self.data = data

    def __init__(self, fan_out_callback):
        self.input_queue = queue.Queue()
        self.output_queue = queue.Queue()
        self.local_client_thread = threading.Thread(target=FanOutServer.run_local_client,
                                                    args=(self.output_queue, self.input_queue, fan_out_callback))
        self.local_client_thread.start()

    def process(self, xs):
        ys = [None] * len(xs)
        completed = 0

        while completed < len(xs):
            for index, item in enumerate(xs):
                if ys[index] is None:
                    self.input_queue.put(self.DataWithIndex(index, item))

            try:
                output = self.output_queue.get(timeout=1)
                if ys[output.index] is None:
                    ys[output.index] = output.data
                    completed += 1
            except queue.Empty:
                continue

    @staticmethod
    def run_local_client(output_queue: queue.Queue, input_queue: queue.Queue, callback):
        while True:
            input = input_queue.get()
            output_data = callback(input.data)
            output_queue.put(FanOutServer.DataWithIndex(input.index, output_data))

class DataX:
    def __init__(self, fan_out_callback=None):
        sidecar_address = os.getenv("DATAX_SIDECAR_ADDRESS", "127.0.0.1:20001")
        self.channel = grpc.insecure_channel(sidecar_address)
        self.stub = DataXStub(self.channel)
        self.fan_out_server = None
        if fan_out_callback is not None:
            self.fan_out_server = FanOutServer(fan_out_callback)
        if fan_out_callback is not None and os.getenv("DATAX_FAN_OUT_HANDLER") == "true":
            self._run_fan_out_handler(fan_out_callback)

    @staticmethod
    def get_configuration() -> dict:
        configuration_path = os.getenv('DATAX_CONFIGURATION')
        if configuration_path is None or configuration_path == '':
            configuration_path = "/datax/configuration"
        with open(configuration_path, 'r') as f:
            return json.load(f)

    def next(self) -> (str, str, dict):
        response = self.stub.Next(NextOptions())
        return response.stream, response.reference, msgpack.unpackb(response.data)

    def emit(self, message: dict, reference: str = None):
        data = msgpack.packb(message)
        if reference is not None:
            request = EmitMessage(data=data, reference=reference)
        else:
            request = EmitMessage(data=msgpack.packb(message))
        self.stub.Emit(request)

    def reply(self, message: dict):
        data = msgpack.packb(message)
        self.stub.ReplyRequest(Reply(data=[data]))

    def fan_out(self, messages: list):
        return self.fan_out_server.process(messages)

    def _run_fan_out_handler(self, callback):
        opts = NextOptions()

        while True:
            res = self.stub.Next(opts)
            output = callback(pickle.loads(res.data))
            self.stub.Emit(EmitMessage(data=pickle.dumps(output)))
