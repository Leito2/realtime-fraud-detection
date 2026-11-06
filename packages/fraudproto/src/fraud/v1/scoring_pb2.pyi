import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Channel(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CHANNEL_UNSPECIFIED: _ClassVar[Channel]
    CHANNEL_WEB: _ClassVar[Channel]
    CHANNEL_APP: _ClassVar[Channel]
    CHANNEL_POS: _ClassVar[Channel]

class Decision(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DECISION_UNSPECIFIED: _ClassVar[Decision]
    DECISION_APPROVE: _ClassVar[Decision]
    DECISION_REVIEW: _ClassVar[Decision]
    DECISION_BLOCK: _ClassVar[Decision]
CHANNEL_UNSPECIFIED: Channel
CHANNEL_WEB: Channel
CHANNEL_APP: Channel
CHANNEL_POS: Channel
DECISION_UNSPECIFIED: Decision
DECISION_APPROVE: Decision
DECISION_REVIEW: Decision
DECISION_BLOCK: Decision

class ScoreRequest(_message.Message):
    __slots__ = ("payment_id", "user_id", "card_id", "amount", "currency", "merchant_id", "mcc", "country", "device_id", "ip_country", "channel", "event_time")
    PAYMENT_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    CARD_ID_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_FIELD_NUMBER: _ClassVar[int]
    CURRENCY_FIELD_NUMBER: _ClassVar[int]
    MERCHANT_ID_FIELD_NUMBER: _ClassVar[int]
    MCC_FIELD_NUMBER: _ClassVar[int]
    COUNTRY_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    IP_COUNTRY_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIME_FIELD_NUMBER: _ClassVar[int]
    payment_id: str
    user_id: str
    card_id: str
    amount: float
    currency: str
    merchant_id: str
    mcc: str
    country: str
    device_id: str
    ip_country: str
    channel: Channel
    event_time: _timestamp_pb2.Timestamp
    def __init__(self, payment_id: _Optional[str] = ..., user_id: _Optional[str] = ..., card_id: _Optional[str] = ..., amount: _Optional[float] = ..., currency: _Optional[str] = ..., merchant_id: _Optional[str] = ..., mcc: _Optional[str] = ..., country: _Optional[str] = ..., device_id: _Optional[str] = ..., ip_country: _Optional[str] = ..., channel: _Optional[_Union[Channel, str]] = ..., event_time: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ScoreResponse(_message.Message):
    __slots__ = ("payment_id", "decision", "score", "model_version", "threshold_set", "degraded", "server_latency_us")
    PAYMENT_ID_FIELD_NUMBER: _ClassVar[int]
    DECISION_FIELD_NUMBER: _ClassVar[int]
    SCORE_FIELD_NUMBER: _ClassVar[int]
    MODEL_VERSION_FIELD_NUMBER: _ClassVar[int]
    THRESHOLD_SET_FIELD_NUMBER: _ClassVar[int]
    DEGRADED_FIELD_NUMBER: _ClassVar[int]
    SERVER_LATENCY_US_FIELD_NUMBER: _ClassVar[int]
    payment_id: str
    decision: Decision
    score: float
    model_version: str
    threshold_set: str
    degraded: bool
    server_latency_us: int
    def __init__(self, payment_id: _Optional[str] = ..., decision: _Optional[_Union[Decision, str]] = ..., score: _Optional[float] = ..., model_version: _Optional[str] = ..., threshold_set: _Optional[str] = ..., degraded: _Optional[bool] = ..., server_latency_us: _Optional[int] = ...) -> None: ...

class GetDecisionRequest(_message.Message):
    __slots__ = ("payment_id",)
    PAYMENT_ID_FIELD_NUMBER: _ClassVar[int]
    payment_id: str
    def __init__(self, payment_id: _Optional[str] = ...) -> None: ...
