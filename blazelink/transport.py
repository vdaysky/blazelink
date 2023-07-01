import asyncio
import random
from asyncio import Future
from enum import IntEnum
from typing import Callable, Any

from pydantic import BaseModel, ValidationError
from starlette.websockets import WebSocket, WebSocketDisconnect

from blazelink import schemas
from blazelink.debugger import Debugger
from blazelink.models import ObjectId
from blazelink.schemas import ConfirmEvent, PingEvent, PingInEvent, AuthorizeEvent, ModelUpdateEvent, \
    IncomingEvent


class UpdateType(IntEnum):
    Create = 1
    Update = 2
    Delete = 3
    Dependency = 4


class Transport:

    _response_futures = {}

    def get_session_id(self) -> str:
        raise NotImplementedError()

    async def push_update(self, update_type: UpdateType, identifier: ObjectId, update_id: int):
        raise NotImplementedError()

    async def send_message(self, message: BaseModel, msg_id: int = None, blocking=True):
        raise NotImplementedError


class StarletteTransport(Transport):

    def __init__(self, websocket: WebSocket, session_id: str, debugger: Debugger):
        self.websocket = websocket
        self.session_id = session_id
        self.debugger = debugger

        self._on_authorize_handlers = []

        # make sure next message is not sent before previous is confirmed
        # self.message_lock = asyncio.Lock()

    def get_session_id(self) -> str:
        return self.session_id

    def on_authorize(self, func: Callable[[AuthorizeEvent], Any]):
        self._on_authorize_handlers.append(func)
        return func

    async def push_update(self, update_type: UpdateType, identifier: ObjectId, update_id: int):
        print("Sending update...", update_type, identifier)

        await self.debugger.message_sent(self.session_id, update_id, identifier)

        await self.send_message(
            ModelUpdateEvent(
                update_type=update_type.name,
                identifier=identifier.dict()
            )
        )
        print("Update sent!")

    async def run(self):
        """ Run transport loop. Has limited support for internal websocket events, like confirming
         incoming websocket events, sending pings and so on. For other events HTTP API should be used.
         NOTE: DO NOT USE blocking send_message here. Send message will wait for confirmation,
         and confirmation only comes from this loop. """

        while True:
            try:
                message = await self.websocket.receive_json()
            except WebSocketDisconnect as e:
                print("Websocket disconnected raised by receive_json", e, "reason", e.reason)
                break

            try:
                event = IncomingEvent(**message)
            except ValidationError as e:
                print("Could not parse websocket event:", message, e)
                continue

            event_cls = getattr(schemas, event.type, None)

            if event_cls is None:
                print("Unexpected event type: ", event.type)
                continue

            event_data = event_cls(**event.data)

            if isinstance(event_data, ConfirmEvent):
                if event_data.confirm_message_id in self._response_futures:
                    self._response_futures[event_data.confirm_message_id].set_result(None)
                    self._response_futures.pop(event_data.confirm_message_id)
                else:
                    print("Unhandled message", message)

            if isinstance(event_data, PingEvent):
                # do not block to avoid deadlock
                await self.send_message(
                    PingInEvent(
                        ping_id=event_data.ping_id
                    ),
                    blocking=False
                )

            if isinstance(event_data, AuthorizeEvent):
                for handler in self._on_authorize_handlers:
                    handler(event_data)

    async def send_message(self, message: BaseModel, msg_id: int = None, blocking=True):
        """ Send message to client and wait for response with matching msg_id. Returns None """

        # await self.message_lock.acquire()

        if msg_id is None:
            msg_id = random.randrange(1, 100_000)

        message_dict = {
            "type": message.__class__.__name__,
            "msg_id": msg_id,
            "data": message.dict()
        }

        future = Future()
        self._response_futures[msg_id] = future
        await self.websocket.send_json(message_dict)

        if blocking:
            print(f"Waiting for future {msg_id}.... ({message_dict})")
            try:
                await asyncio.wait_for(future, timeout=60)
            except asyncio.TimeoutError:
                print(f"future {msg_id} timed out! No response from client.")
            else:
                print(f"future {msg_id} Done!")

        # self.message_lock.release()
