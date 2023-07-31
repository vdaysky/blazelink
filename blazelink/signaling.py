from __future__ import annotations
import json
import random
from dataclasses import dataclass

from psycopg2.extras import ReplicationMessage

from blazelink import TableManager
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

    def __init__(self, subs: SubscriptionManager, table_manager: TableManager, db_host, db_port, db_name, db_user, db_password, slot_name: str, debugger: Debugger):
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.table_manager = table_manager
        self.subs = subs
        self.debugger = debugger
        self.slot_name = slot_name

    async def start(self):
        message: ReplicationMessage
        async for message in read_events(self.db_host, self.db_port, self.db_name, self.db_user, self.db_password, self.slot_name):
            data = json.loads(message.payload)

            print("Replication message:", data)

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

                    # sanitize data by only including fields present on the model
                    if data:
                        for model in self.table_manager.models:

                            model_name = model.get_type_name().replace("_", "").lower()
                            table_name = table.replace("_", "").lower()

                            if model_name == table_name:
                                fields = model.get_model_props(self.table_manager.models)
                                explicit_field_set = set([f.name for f in fields])
                                clear_data = {}
                                for key in data:
                                    if key in explicit_field_set:
                                        clear_data[key] = data[key]
                                data = clear_data
                                break

                    update = Update(update_id, kind, table, pk, data)
                    print("Update:", update)
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
