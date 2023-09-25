import pickle

import grpc
import json
import os
import msgpack
from .protocol.datax_sdk_protocol_pb2 import NextOptions, EmitMessage, Request, GetRequestOptions, Reply
from .protocol.datax_sdk_protocol_pb2_grpc import DataXStub


class DataX:
    def __init__(self, fan_out_callback=None):
        sidecar_address = os.getenv("DATAX_SIDECAR_ADDRESS", "127.0.0.1:20001")
        self.channel = grpc.insecure_channel(sidecar_address)
        self.stub = DataXStub(self.channel)
        self.fan_out_callback = fan_out_callback
        if fan_out_callback is not None and os.getenv("DATAX_FAN_OUT_HANDLER") is not "":
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
        return [self.fan_out_callback(x) for x in messages]

    def _run_fan_out_handler(self, callback):
        opts = NextOptions()

        while True:
            res = self.stub.Next(opts)
            output = callback(pickle.loads(res.data))
            self.stub.Emit(EmitMessage(data=pickle.dumps(output)))
