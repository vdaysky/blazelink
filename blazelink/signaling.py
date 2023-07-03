from __future__ import annotations
import json
import random
from dataclasses import dataclass

from psycopg2.extras import ReplicationMessage

from blazelink.debugger import Debugger
from blazelink.monitoring import read_events
from blazelink.subscription import SubscriptionManager


@dataclass
class Update:

    id: int
    kind: str
    table: str
    pk: int
    data: dict

    cause: Update | None = None


class Signaler:

    def __init__(self, subs: SubscriptionManager, data_accessor, db_host, db_port, db_name, db_user, db_password, debugger: Debugger):
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.subs = subs
        self.data_accessor = data_accessor
        self.debugger = debugger

    async def start(self):
        message: ReplicationMessage
        async for message in read_events(self.db_host, self.db_port, self.db_name, self.db_user, self.db_password):
            data = json.loads(message.payload)

            changes = data['change']

            for change in changes:
                kind = change['kind']

                if kind not in {'insert', 'update', 'delete'}:
                    continue

                table = change['table']
                columns = change.get('columnnames')
                values = change.get('columnvalues')

                if kind == 'insert':
                    # todo support different names
                    pk = values[columns.index('id')]
                else:
                    oldkeys = change.get("oldkeys")
                    assert len(oldkeys['keyvalues']), "Composite keys are not yet supported"
                    pk = oldkeys['keyvalues'][0]

                if kind in {'insert', 'update'}:
                    data = {k: v for k, v in zip(columns, values)}
                else:
                    # delete does not have data
                    data = None

                try:
                    update_id = random.randint(0, 1000000000)
                    update = Update(update_id, kind, table, pk, data)

                    await self.debugger.log_update(update)

                    await self.subs.push_raw_update(update)

                    # for _table, _pk in self.data_accessor.get_referenced_tables(table, pk):
                    #
                    #     caused_update_id = random.randint(0, 1000000000)
                    #     caused_update = Update(caused_update_id, "update", _table, _pk, {}, cause=update)
                    #
                    #     await self.subs.push_raw_update(caused_update)

                except Exception as e:
                    import traceback
                    traceback.print_exc()
