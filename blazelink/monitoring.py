import json
from asyncio import Future
from typing import AsyncGenerator, Optional

import asyncio

import psycopg2
from psycopg2.extras import LogicalReplicationConnection, ReplicationCursor, ReplicationMessage
from psycopg2.extensions import connection

REPLICATION_SLOT = "betterms"


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


async def _create_replication_slot_if_not_exists(cursor: ReplicationCursor, override=False) -> None:
    """Helper function to create a replication slot if it doesn't exist.
    The replication slot is a feature in PostgreSQL that ensures that
    the master server will retain the WALs (Write-Ahead Log)s.
    In PostgreSQL, the WAL is also known as transaction log.
    A log is a record of all the events or changes and WAL data
    is just a description of changes made to the actual data.
    Args:
        cursor: The cursor object to execute the queries.
    """
    # Query the pg_replication_slots view to check if the replication slot exists
    cursor.execute(
        f"SELECT slot_name FROM pg_replication_slots WHERE slot_name = '{REPLICATION_SLOT}';"
    )

    # Wait for the result of the query to be ready
    await connection_ready(cursor.connection)

    slot_exists: bool = cursor.fetchone() is not None

    if slot_exists and override:
        cursor.drop_replication_slot(REPLICATION_SLOT)
        slot_exists = False

    if not slot_exists:
        cursor.create_replication_slot(
            REPLICATION_SLOT, output_plugin="wal2json"
        )

    await connection_ready(cursor.connection)


async def read_events(host, port, database, user, password) -> AsyncGenerator[str, None]:

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
        await _create_replication_slot_if_not_exists(_cursor)

        _cursor.start_replication(
            slot_name=REPLICATION_SLOT,
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


async def test():
    message: ReplicationMessage
    async for message in read_events(host="127.0.0.1", port=5432, user="vova", password=1, database="blazelink"):
        print(json.loads(message.payload))


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(test())
