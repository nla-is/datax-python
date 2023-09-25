import time
import json
import os
import pickle
import platform
import queue
import sys
import threading
from concurrent import futures
from dataclasses import dataclass

import grpc
import msgpack

from .protocol.datax_sdk_protocol_pb2 import NextOptions, EmitMessage, FanOutDataItem, Reply, Empty
from .protocol.datax_sdk_protocol_pb2_grpc import DataXStub, FanOutServicer, FanOutStub, add_FanOutServicer_to_server


class FanOutServer(FanOutServicer):
    @dataclass
    class DataItem:
        task_id: str
        data_item_id: int
        data: None

    def __init__(self, fan_out_callback):
        self.task_id_prefix = f"{platform.node()}--{int(time.time())}"
        self.next_task_id = 0
        self.input_queue = queue.Queue()
        self.output_queue = queue.Queue()
        self.local_client_thread = threading.Thread(target=FanOutServer.run_local_client,
                                                    args=(self.output_queue, self.input_queue, fan_out_callback))
        self.local_client_thread.start()
        self.total_data_item = 0
        self.data_item_processed_locally = 0
        self.data_item_double_processed = 0
        self.latest_report = time.time()

    def GetInput(self, request, context):
        data_item = self.input_queue.get()
        self.input_queue.put(data_item)
        return FanOutDataItem(task_id=data_item.task_id, data_item_id=data_item.data_item_id,
                              data=pickle.dumps(data_item.data))

    def SetOutput(self, request, context):
        data_item = self.DataItem(request.task_id, request.data_item_id, pickle.loads(request.data))
        self.output_queue.put(data_item)
        return Empty()

    def start(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
        add_FanOutServicer_to_server(self, server)
        server.add_insecure_port("0.0.0.0:20200")
        server.start()
        print("FanOut Server started")
        threading.Thread(target=self.wait_for_termination, args=(server,)).start()

    @staticmethod
    def wait_for_termination(server):
        server.wait_for_termination()
        print(f"Server exited")
        time.sleep(3)
        sys.exit(-1)

    def process(self, xs):
        now = time.time()
        if now - self.latest_report >= 5:
            print(f"[DataX] FanOut data items: {self.total_data_item}, double processed: {self.data_item_double_processed}")
            self.latest_report = now

        task_id = f"{self.task_id_prefix}--{self.next_task_id}"
        self.next_task_id += 1

        ys = [None] * len(xs)
        completed = 0

        self.total_data_item += len(xs)

        for index, item in enumerate(xs):
            if ys[index] is None:
                self.input_queue.put(self.DataItem(task_id, index, item))

        while completed < len(xs):
            try:
                output: FanOutServer.DataItem = self.output_queue.get(timeout=1)
                if output.task_id != task_id:
                    self.data_item_double_processed += 1
                    # print(f"Warning received output from different task {output.task_id}")
                    continue
                self.input_queue.mutex.acquire()
                updated = True
                while updated:
                    updated = False
                    for i, item in enumerate(self.input_queue.queue):
                        if item.task_id == output.task_id and item.data_item_id == output.data_item_id:
                            del self.input_queue.queue[i]
                            updated = True
                            break
                self.input_queue.mutex.release()
                if ys[output.data_item_id] is None:
                    ys[output.data_item_id] = output.data
                    completed += 1
            except queue.Empty:
                continue

        while True:
            try:
                self.input_queue.get_nowait()
            except queue.Empty:
                break

        return ys

    @staticmethod
    def run_local_client(output_queue: queue.Queue, input_queue: queue.Queue, callback):
        while True:
            data_item: FanOutServer.DataItem = input_queue.get()
            output_data = callback(data_item.data)
            output_queue.put(FanOutServer.DataItem(data_item.task_id, data_item.data_item_id, output_data))


class FanOutHandler(object):
    def __init__(self, callback, server_address):
        self.callback = callback
        self.server_address = server_address
        self.channel: grpc.Channel = None
        self.stub: FanOutStub = None

    def run(self):
        while True:
            if self.stub is None:
                self._reconnect()
            print("Obtaining input")
            data_item: FanOutDataItem = self.stub.GetInput(Empty())
            print("Processing")
            output = self.callback(pickle.loads(data_item.data))
            print("Sending output")
            self.stub.SetOutput(FanOutDataItem(task_id=data_item.task_id, data_item_id=data_item.data_item_id,
                                               data=pickle.dumps(output)))

    def _reconnect(self):
        connected = False
        while not connected:
            if self.channel is not None:
                self.channel.close()

            self.channel = grpc.insecure_channel(self.server_address)
            self.stub = FanOutStub(self.channel)
            connected = True


class DataX:
    def __init__(self, fan_out_callback=None):
        self.fan_out_server = None
        if fan_out_callback is not None:
            fan_out_server = os.getenv("DATAX_SIDECAR_FAN_OUT_SERVER", "")
            if fan_out_server != "":
                FanOutHandler(fan_out_callback, fan_out_server).run()
                sys.exit(-1)
            self.fan_out_server = FanOutServer(fan_out_callback)
            self.fan_out_server.start()
        sidecar_address = os.getenv("DATAX_SIDECAR_ADDRESS", "127.0.0.1:20001")
        self.channel = grpc.insecure_channel(sidecar_address)
        self.stub = DataXStub(self.channel)

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
