import json
from asyncio import Future
from typing import AsyncGenerator, Optional

import asyncio

import psycopg2
from psycopg2.extras import LogicalReplicationConnection, ReplicationCursor, ReplicationMessage
from psycopg2.extensions import connection


async def connection_ready(conn: connection) -> None:

    while True:
        state: int = conn.poll()

        if state == psycopg2.extensions.POLL_OK:
            break

        elif state == psycopg2.extensions.POLL_WRITE:
            await wait_write_ready(conn.fileno())

        elif state == psycopg2.extensions.POLL_READ:
            await wait_read_ready(conn.fileno())

        elif state == psycopg2.extensions.POLL_ERROR:
            raise psycopg2.OperationalError("POLL_ERROR")


async def wait_read_ready(descriptor: int):
    """Wait until the descriptor is ready for reading.
    Args:
        descriptor: The file descriptor to wait for.
    """

    future = Future()

    def callback():
        future.set_result(None)
        asyncio.get_event_loop().remove_reader(descriptor)

    asyncio.get_event_loop().add_reader(descriptor, callback)
    await future


async def wait_write_ready(descriptor):
    """Wait until the descriptor is ready for writing.
    Args:
        descriptor: The file descriptor to wait for.
    """
    future = Future()

    def callback():
        future.set_result(None)
        asyncio.get_event_loop().remove_writer(descriptor)

    asyncio.get_event_loop().add_writer(descriptor, callback)
    await future


REPLICATION_OPTIONS: dict[str, str] = {
    "include-xids": "1",
    "include-timestamp": "1",
}


async def ensure_slot_exists(cursor: ReplicationCursor, slot_name, override=False) -> None:

    cursor.execute(
        f"SELECT slot_name FROM pg_replication_slots WHERE slot_name = '{slot_name}';"
    )

    # Wait for the result of the query to be ready
    await connection_ready(cursor.connection)

    slot_exists: bool = cursor.fetchone() is not None

    if slot_exists and override:
        cursor.drop_replication_slot(slot_name)
        slot_exists = False

    if not slot_exists:
        cursor.create_replication_slot(
            slot_name, output_plugin="wal2json"
        )

    await connection_ready(cursor.connection)


async def read_events(host, port, database, user, password, slot_name) -> AsyncGenerator[str, None]:

    conn: connection = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
        connection_factory=psycopg2.extras.LogicalReplicationConnection,
        async_=True,
    )

    await connection_ready(conn)

    _cursor: ReplicationCursor

    with conn.cursor() as _cursor:

        await ensure_slot_exists(_cursor, slot_name)

        _cursor.start_replication(
            slot_name=slot_name,
            decode=True,
            options=REPLICATION_OPTIONS,
        )

        await connection_ready(conn)

        while True:
            await wait_read_ready(_cursor.fileno())

            message: Optional[ReplicationMessage] = _cursor.read_message()

            if message is None:
                continue

            message.cursor.send_feedback(flush_lsn=message.data_start)

            yield message
