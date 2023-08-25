import grpc
import json
import os
import msgpack
from .protocol.datax_sdk_protocol_pb2 import NextOptions, EmitMessage, Request, GetRequestOptions, Reply
from .protocol.datax_sdk_protocol_pb2_grpc import DataXStub


class DataX:
    def __init__(self, ):
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

    def request(self, backend: str, message: dict):
        data = msgpack.packb(message)
        reply = self.stub.SubmitRequest(Request(backend=backend, data=[data]))
        return msgpack.unpackb(reply.data[0])

    def request_many(self, backend: str, messages: list):
        data = [msgpack.packb(it) for it in messages]
        reply = self.stub.SubmitRequest(Request(backend=backend, data=data))
        return [msgpack.unpackb(it) for it in reply.data]

    def next_request(self) -> (str, dict):
        request = self.stub.GetRequest(GetRequestOptions())
        return request.sender, msgpack.unpackb(request.data[0])

    def reply(self, message: dict):
        data = msgpack.packb(message)
        self.stub.ReplyRequest(Reply(data=[data]))
