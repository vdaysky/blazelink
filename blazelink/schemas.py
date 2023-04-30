from pydantic import BaseModel


class IncomingEvent(BaseModel):
    type: str
    data: dict


class PingEvent(BaseModel):
    ping_id: int


class ConfirmEvent(BaseModel):
    confirm_message_id: int
    payload: dict


class AuthorizeEvent(BaseModel):
    secret: str
    handle: str


class PingInEvent(BaseModel):
    ping_id: int


class ModelUpdateEvent(BaseModel):
    update_type: str
    identifier: dict
