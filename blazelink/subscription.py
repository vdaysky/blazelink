import logging
from collections import defaultdict

from blazelink.connection import ConnectionManager
from blazelink.models import ObjectId
from blazelink.transport import Transport, UpdateType


class SubscriptionManager:

    def __init__(self, connections: ConnectionManager):
        self.conns = connections

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

    async def push_raw_update(self, kind: str, table: str, pk: int, data: dict):

        # map database table to gql table
        # temporary impl
        entity = table.capitalize()

        # ID that was just updated
        obj_id = ObjectId(obj_id=pk, entity=entity, dependencies=[])

        update_type = {
            'update': UpdateType.Update,
            'insert': UpdateType.Create,
            'delete': UpdateType.Delete
        }

        await self.push_recursive_update(update_type[kind], obj_id)

    async def push_single_update(self, update_type: UpdateType, identifier: ObjectId):

        print("[Push Single update]", update_type, identifier)
        to_delete = set()

        for conn_id, identifiers in {**self._subscriptions}.items():

            if identifier in identifiers:
                connection = self.conns.get_connection(conn_id)
                if not connection:
                    to_delete.add(conn_id)
                    logging.warning(f"Connection for session {conn_id} not found")
                    continue

                await connection.push_update(update_type, identifier)

        for key in to_delete:
            self._subscriptions.pop(key)

    async def push_recursive_update(self, update_type: UpdateType, identifier: ObjectId, seen: set[ObjectId] = None):

        if seen is None:
            seen = set()

        if identifier in seen:
            return

        seen.add(identifier)

        await self.push_single_update(update_type, identifier)

        for dependant in self.find_dependants(identifier):
            await self.push_recursive_update(UpdateType.Dependency, dependant, seen=seen)
