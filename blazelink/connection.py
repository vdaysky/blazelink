import asyncio

from blazelink.schemas import AuthorizeEvent
from blazelink.transport import Transport


class ConnectionManager:

    def __init__(self):
        self._connections: dict[str, Transport] = {}
        # Transport by handle
        self._authorized_connections = {}
        self._on_handle_authorized_handlers = {}

    def add_connection(self, connection: Transport):
        self._connections[connection.get_session_id()] = connection

    def get_connection(self, connection_id: str) -> Transport | None:
        return self._connections.get(connection_id)

    def remove_connection(self, connection: Transport):
        self._connections.pop(connection.get_session_id(), None)

        for handle, conn in self._authorized_connections.items():
            if conn == connection:
                self._authorized_connections.pop(handle, None)
                break

    def get_authorized(self, handle: str) -> Transport | None:
        return self._authorized_connections.get(handle)

    def authorize(self, transport: Transport, event: AuthorizeEvent):
        # todo: actually authorize
        self._authorized_connections[event.handle] = transport

        for handler in self._on_handle_authorized_handlers.get(event.handle, []):
            result = handler(transport)
            if asyncio.iscoroutine(result):
                asyncio.create_task(result)

    def on_authorized(self, handle: str):
        def decorator(func):
            self._on_handle_authorized_handlers.setdefault(handle, []).append(func)
            return func
        return decorator
