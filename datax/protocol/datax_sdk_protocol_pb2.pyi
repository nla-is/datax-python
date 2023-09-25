from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class Settings(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class Initialization(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class NextOptions(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class NextMessage(_message.Message):
    __slots__ = ["reference", "stream", "data"]
    REFERENCE_FIELD_NUMBER: _ClassVar[int]
    STREAM_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    reference: str
    stream: str
    data: bytes
    def __init__(self, reference: _Optional[str] = ..., stream: _Optional[str] = ..., data: _Optional[bytes] = ...) -> None: ...

class EmitMessage(_message.Message):
    __slots__ = ["data", "reference"]
    DATA_FIELD_NUMBER: _ClassVar[int]
    REFERENCE_FIELD_NUMBER: _ClassVar[int]
    data: bytes
    reference: str
    def __init__(self, data: _Optional[bytes] = ..., reference: _Optional[str] = ...) -> None: ...

class EmitResult(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class GetRequestOptions(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class Request(_message.Message):
    __slots__ = ["sender", "backend", "data"]
    SENDER_FIELD_NUMBER: _ClassVar[int]
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    sender: str
    backend: str
    data: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, sender: _Optional[str] = ..., backend: _Optional[str] = ..., data: _Optional[_Iterable[bytes]] = ...) -> None: ...

class Reply(_message.Message):
    __slots__ = ["data"]
    DATA_FIELD_NUMBER: _ClassVar[int]
    data: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, data: _Optional[_Iterable[bytes]] = ...) -> None: ...

class ReplyResult(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class FanOutRequest(_message.Message):
    __slots__ = ["data"]
    DATA_FIELD_NUMBER: _ClassVar[int]
    data: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, data: _Optional[_Iterable[bytes]] = ...) -> None: ...

class FanOutResponse(_message.Message):
    __slots__ = ["data"]
    DATA_FIELD_NUMBER: _ClassVar[int]
    data: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, data: _Optional[_Iterable[bytes]] = ...) -> None: ...
