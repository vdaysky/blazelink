import json

from psycopg2.extras import ReplicationMessage

from blazelink.monitoring import read_events
from blazelink.subscription import SubscriptionManager


class Signaler:

    def __init__(self, subs: SubscriptionManager, data_accessor, db_host, db_port, db_name, db_user, db_password):
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.subs = subs
        self.data_accessor = data_accessor

    async def start(self):
        print("Starting signaler")
        message: ReplicationMessage
        async for message in read_events(self.db_host, self.db_port, self.db_name, self.db_user, self.db_password):
            print("Message received")
            data = json.loads(message.payload)

            print("Message", data)
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
                    print("pushing update for original table", table, "with id", pk)
                    await self.subs.push_raw_update(kind, table, pk, data)

                    for _table, _pk in self.data_accessor.get_referenced_tables(table, pk):
                        print("Pushing update for referenced table", _table, "with id",  _pk)
                        await self.subs.push_raw_update("update", _table, _pk, data)

                except Exception as e:
                    import traceback
                    traceback.print_exc()
