from __future__ import annotations

import logging
from collections import defaultdict

from blazelink.connection import ConnectionManager
from blazelink.debugger import Debugger
from blazelink.models import ObjectId
from blazelink.transport import Transport, UpdateType

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from blazelink.signaling import Update


class SubscriptionManager:

    def __init__(self, connections: ConnectionManager, debugger: Debugger):
        self.conns = connections
        self.debugger = debugger

        self._subscriptions: dict[str, list[ObjectId]] = defaultdict(list)

    def subscribe(self, session_id: str, identifier: ObjectId):
        """ Subscribe given session to given identifier """
        self._subscriptions[session_id].append(identifier)

    def unsubscribe(self, session_id: str, identifier: ObjectId) -> bool:
        """ Unsubscribe given session from given identifier """
        try:
            self._subscriptions[session_id].remove(identifier)
            return True
        except ValueError:
            return False

    def disconnect(self, session_id: str):
        """ Unsubscribe given session from all identifiers """
        self._subscriptions.pop(session_id, None)

    @classmethod
    def find_dependants(cls, identifier: ObjectId) -> set[ObjectId]:
        """ Find all identifiers that depend on given identifier """

        dependants = set(identifier._dependants)

        for object_id, ref_count in ObjectId._registry.values():
            if object_id.does_depend(identifier):
                dependants.add(object_id)

        return dependants

    async def push_raw_update(self, update: Update):

        # map database table to gql table
        # temporary impl
        entity = update.table.capitalize()

        # ID that was just updated
        obj_id = ObjectId(obj_id=update.pk, entity=entity, dependencies=[])

        update_type = {
            'update': UpdateType.Update,
            'insert': UpdateType.Create,
            'delete': UpdateType.Delete
        }

        await self.push_recursive_update(update_type[update.kind], obj_id, update.id)

    async def push_single_update(self, update_type: UpdateType, identifier: ObjectId, update_id: int):
        """ Push single update to all subscribers of given identifier

        :param update_type: Type of update. Can be Update, Create or Delete
        :param identifier: ObjectId of object that was updated
        :param update_id: ID of update for debug
        """

        print("[Push Single update]", update_type, identifier)
        to_delete = set()

        for conn_id, identifiers in {**self._subscriptions}.items():

            await self.debugger.record_connection_check(conn_id, update_id)

            if identifier in identifiers:

                await self.debugger.record_subscription_found(conn_id, update_id, identifier)

                connection = self.conns.get_connection(conn_id)
                if not connection:
                    to_delete.add(conn_id)
                    logging.warning(f"Connection for session {conn_id} not found")
                    continue

                await connection.push_update(update_type, identifier, update_id)

            else:
                await self.debugger.record_subscription_not_found(conn_id, update_id, identifier)

        for key in to_delete:
            await self.debugger.record_subscription_deleted(key, update_id)
            self._subscriptions.pop(key)

    async def push_recursive_update(self, update_type: UpdateType, identifier: ObjectId, update_id: int, seen: set[ObjectId] = None):
        """ Push update to all subscribers of given identifier and all subscribers of entities depending on this object

        :param update_type: Type of update. Can be Update, Create or Delete
        :param identifier: ObjectId of object that was updated
        :param update_id: ID of update for debug
        :param seen: Set of already processed identifiers
        """

        if seen is None:
            seen = set()

        if identifier in seen:
            return

        seen.add(identifier)

        await self.push_single_update(update_type, identifier, update_id)

        for dependant in self.find_dependants(identifier):
            await self.push_recursive_update(UpdateType.Dependency, dependant, update_id, seen=seen)

