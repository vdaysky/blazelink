from __future__ import annotations

from typing import Any, TYPE_CHECKING

from pydantic import BaseModel

if TYPE_CHECKING:
    from blazelink.signaling import Update
    from blazelink.transport import Transport
    from blazelink.models import ObjectId


class Debugger:

    def __init__(self):
        self.monitoring_conn: Transport | None = None
        # self.queued_messages = []
    def set_transport(self, transport: Transport):
        self.monitoring_conn = transport

    async def send_message(self, action: str, update_id: int, connection_id: str | None, payload):

        class DebugMessage(BaseModel):
            update_id: int | None
            action: str
            connection_id: str | None
            payload: Any
        message = DebugMessage(
            update_id=update_id,
            action=action,
            connection_id=connection_id,
            payload=payload
        )

        if self.monitoring_conn is not None:
            await self.monitoring_conn.send_message(message, blocking=False)

    async def log_update(self, update: Update):
        """ Record an update object """
        await self.send_message('log_update', update.id, None, {
            "cause_id": update.cause.update_id if update.cause else None,
            "table": update.table,
            "pk": update.pk,
            "kind": update.kind,
        })

    # region Client-based events

    async def sync_subscriptions(self, conn_id, subs: list[ObjectId]):
        await self.send_message('sync_subscriptions', None, conn_id, {
            "object_ids": [sub.dict() for sub in subs]
        })

    async def add_subscription(self, session_id, object_id):
        """ Event message was sent to given session to notify of update """
        await self.send_message('add_subscription', None, session_id, {'object_id': object_id.dict()})

    # endregion

    async def record_connection_check(self, conn_id, update_id):
        """ Record check of given connection id by update id """
        await self.send_message('record_connection_check', update_id, conn_id, {})

    async def record_subscription_found(self, conn_id, update_id, identifier: ObjectId):
        """ ObjectId updated by given update was found as a subscription in given connection """
        await self.send_message('record_subscription_found', update_id, conn_id, {'object_id': identifier.dict()})

    async def record_subscription_not_found(self, conn_id, update_id, identifier: ObjectId):
        """ ObjectId updated by given update was not found as a subscription in given connection """
        await self.send_message('record_subscription_not_found', update_id, conn_id, {'object_id': identifier.dict()})

    async def record_subscription_deleted(self, key, update_id):
        """ Connection was deleted while processing given update """
        await self.send_message('record_subscription_deleted', update_id, key, {})

    async def message_sent(self, session_id, update_id, object_id: ObjectId):
        """ Event message was sent to given session to notify of update """
        await self.send_message('message_sent', update_id, session_id, {"object_id": object_id.dict()})


