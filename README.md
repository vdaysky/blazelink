# Blazelink (POC)

[![State-of-the-art Shitcode](https://img.shields.io/static/v1?label=State-of-the-art&message=Shitcode&color=7B5804)](https://github.com/trekhleb/state-of-the-art-shitcode)

## ⚠️ Disclaimer: Early-early not work look shit ⚠️

---

## A realtime event-driven client-server data synchronization framework

Blazelink allows you to write declarative code that removes the need to make countless GET calls or implement websocket 
messaging to keep client state up to date.

[<img src="assets/preview.png" />](https://www.youtube.com/watch?v=BS2sfcRPxFM)

## Technologies

Blazelink backend is functioning as an asgi application and can be mounted to any python asgi server. You may use any ORM 
with this framework as well, as long as you implement a certain interface, described below.

It uses PostgreSQL as a database with enabled logical replication to produce WALs.

## Modules

<img src="assets/services.png">

## Example

Let's make a simple example. For this example I will be using FastAPI with sqlmodel orm. Let's say we have a shooter 
game, in which there are users, games, teams and game events.

First we would create FastAPI app and db conenction:

```python 
app = FastAPI()

engine = create_engine(
    "postgresql://user:pass@localhost:5432/database",
    pool_size=20,
    max_overflow=0,
)

session_maker = sessionmaker(engine, expire_on_commit=False, class_=Session)
```

Now let's define our models:

```python

class User(SQLModel, table=True):
    __tablename__ = "user"
    id: int | None = Field(primary_key=True)
    name: str = Field(max_length=100)
    events: list["GameEvent"] = Relationship(back_populates="player")


class Team(SQLModel, table=True):
    __tablename__ = "team"
    id: int | None = Field(primary_key=True)
    name: str = Field(max_length=100)


class Game(SQLModel, table=True):
    id: int | None = Field(primary_key=True)
    name: str = Field(max_length=100)
    events: list["GameEvent"] = Relationship(back_populates="game")


class GameEvent(SQLModel, table=True):

    id: int | None = Field(primary_key=True)
    type: str = Field(max_length=100)

    player_id: int = Field(foreign_key="user.id")
    game_id: int = Field(foreign_key="game.id")

    game: Game = Relationship(back_populates="events")
    player: User = Relationship(back_populates="events")
```

Now it's time to initialize blazelink server:

First let's implement database accessor:

```python
from blazelink.models import DataAccessor, BlazeContext, GenericQueryContainer, ListQuery

class SqlAlchemyAccessor(DataAccessor):

    async def get_by_pk(self, context: BlazeContext, model: Type[Any], pk: Any) -> Any:
        stmt = select(model).where(model.id == pk)
        return context.session.execute(stmt).scalar_one()

    def execute_query(self, context: BlazeContext, query: GenericQueryContainer) -> Any:
        if isinstance(query, ListQuery):
            return context.session.execute(query.query).all()
        else:
            return context.session.execute(query.query).first()
```

This interface allows blazelink to do two things: 
1. resolve models by primary key
2. execute arbitrary queries that you may return from field resolvers. This will be helpful to allow blazelink modify 
those queries in some way before executing, one example of that would be automatic pagination.

Now let's create a table manager, that will be responsible for creating graphql types and resolvers:

```python
from blazelink.models import BlazeContext
from blazelink import TableManager


def context_factory(__info):
    db = __info.context["database_session"]

    request = __info.context.get("request")
    sess_id = request.headers.get('session_id')
    token = request.headers.get('Authorization')

    return BlazeContext(
        user={'token': token},
        request=request,
        session_id=sess_id,
        session=db
    )


table_manager = TableManager(
    BlazeConfig(
        orm_class=float,
        data_accessor=SqlAlchemyAccessor(),
        context_factory=context_factory
    )
)
```

In this example we are initialising table manager with config object. orm_class is not used anyway, I don't know why I 
didn't remove it yet. context_factory is a function that will create context object for every request. 
This context object will be later passed to all resolvers. It has one argument, ariadne info object. Here I am getting 
"database_session" field from ariadne context, which is set later in the example.


Now let's create what's left:

```python
debugger = Debugger()
connection_manager = ConnectionManager()
subs = SubscriptionManager(connection_manager, debugger=debugger)

@table_manager.on_subscribe
async def on_sub(session_id: str, object_id: ObjectId):
    print("Added sub", session_id, object_id)
    await debugger.add_subscription(session_id, object_id)

    subs.subscribe(session_id, object_id)

    await debugger.sync_subscriptions(session_id, subs._subscriptions[session_id])

    for dependency in object_id.dependencies:
        await on_sub(session_id, dependency)

signaler = Signaler(
    subs,
    db_host='localhost',
    db_port=5432,
    db_name='database',
    db_user='user',
    db_password='pass',
    slot_name="blazelink",
    debugger=debugger,
)
```

in this example we created debugger object, don't worry about it for now, a connection manager, subscription manager and 
a signaler. We registered a callback on table manager, that will be called every time a new subscription is created. 
This callback then would register that subscription in subscription manager.


Now let's take care of SQLAlchemy sessions:

```python
class SessionExtension(Extension):

    def request_started(self, context: ContextValue) -> None:
        context["database_session"] = session_maker()

    def request_finished(self, context: ContextValue) -> None:
        context["database_session"].close()
```

This example makes use or ariadne extensions, which provide lifetime hooks for ariadne requests. Here I am creating and 
destroying the session. This is the place where session is put into ariadne context, which was used above.


Now let's declare the tables we want to be queryable:

```python
@table_manager.table
class GameEventTable(Table[GameEvent]):
    id: int
    type: str
    player: UserTable
    game: GameTable


@table_manager.table
class GameTable(Table[Game]):
    id: int
    name: str


@table_manager.table
class UserTable(Table[User]):
    id: int
    name: str
    events: Page[GameEventTable]

    
@table_manager.table
class PlayerStats(VirtualTable):

    def __init__(self, identifier: ObjectId, context: BlazeContext):
        self.player_id = identifier.find_dependency('user').obj_id

    @computed
    def kills(self) -> int:

        stmt = select(func.count(GameEvent.id)).where(
            GameEvent.type == "kill",
            GameEvent.player_id == self.player_id
        )
        
        return self.context.session.execute(stmt).scalar_one()
```

Here we declared 4 tables, 3 of them are just normal tables, and one is a virtual table. Normal tables are representing
database tables, their generic argument is ORM model class. To declare a field on table, you need to annotate it with a 
type. Possible types are int, str, bool, Page[T], list[T], or Table/Struct/Virtual registered types. To avoid problems 
with circular dependencies, where you need model that is declared below, you can use forward references or just 
`from __future__ import annotations`.
Virtual tables are tables that are not backed by database, but are computed on the fly. Think of them as views. Unlike 
normal tables, that would only depend on their primary key, virtual tables can depend on any other table/row. You can
declare dependencies of a type on server side by overriding `def __dependencies__(self) -> List[ObjectId]` method.
That would force client to re-fetch this object every time any of dependencies change. But generally dependencies 
should be declared on client side.

Note that events field on GameEvent has type Page[GameEventTable]. Pagination is built into blazelink. Paginated fields
will be declared with two arguments in graphql, page and size. You don't have to worry about it though. To make a computed 
paginated field, simply annotate resolver function with page type:
```python
    @computed
    def get_paginated_data(self, range_arg: int = 1000) -> Page[int]:
        return list(range(range_arg))
```
you can also add any argument to the resolver function. It will be included in graphql schema.


There is a struct type as well. Struct type allows to create dictionary-like nested field, that does not have to be 
resolved level by level.

```python

class StructA(Struct):
    a: int


class StructB(Struct):
    b: StructA
    c: int

@table_manager.table
class Queryable(VirtualTable):
    
    @computed
    def get_struct(self) -> StructB:
        return StructB(
            b=StructA(
                a=1
            ),
            c=2
        )

```

Now what's left is to mount asgi app and start database listener:

```python
asgi_app = GraphQL(
    create_schema(table_manager),
    debug=True,
    http_handler=GraphQLHTTPHandler(
        extensions=[
            SessionExtension
        ]
    )
)

app.mount(
    "/graphql",
    asgi_app
)

@app.on_event("startup")
async def start_things():
    asyncio.create_task(signaler.start())


@app.websocket("/ws/connect")
async def websocket_endpoint(websocket: WebSocket, session_id: str):
    await websocket.accept()
    transport = StarletteTransport(websocket, session_id=session_id, debugger=debugger)

    @transport.on_authorize
    def on_authorize(event: AuthorizeEvent):
        connection_manager.authorize(transport, event)

    connection_manager.add_connection(transport)

    await transport.run()

    connection_manager.remove_connection(transport)

```

And now we should have fully functioning graphql server, that also serves updates over websockets when client model 
dependency changes.

For more info on JS client implementation, see [blazelink-js](https://github.com/vdaysky/blazelink-js) repo.
