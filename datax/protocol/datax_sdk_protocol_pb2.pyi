from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

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

class Initialization(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class NextMessage(_message.Message):
    __slots__ = ["data", "reference", "stream"]
    DATA_FIELD_NUMBER: _ClassVar[int]
    REFERENCE_FIELD_NUMBER: _ClassVar[int]
    STREAM_FIELD_NUMBER: _ClassVar[int]
    data: bytes
    reference: str
    stream: str
    def __init__(self, reference: _Optional[str] = ..., stream: _Optional[str] = ..., data: _Optional[bytes] = ...) -> None: ...

class NextOptions(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class Reply(_message.Message):
    __slots__ = ["data"]
    DATA_FIELD_NUMBER: _ClassVar[int]
    data: bytes
    def __init__(self, data: _Optional[bytes] = ...) -> None: ...

class ReplyResult(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class Request(_message.Message):
    __slots__ = ["backend", "data", "sender"]
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    SENDER_FIELD_NUMBER: _ClassVar[int]
    backend: str
    data: bytes
    sender: str
    def __init__(self, sender: _Optional[str] = ..., backend: _Optional[str] = ..., data: _Optional[bytes] = ...) -> None: ...

class Settings(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...
