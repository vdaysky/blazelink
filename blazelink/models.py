import asyncio
import inspect
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import TypeVar, Generic, List, get_args, Type, Optional, Union, Callable, Any, get_type_hints, ForwardRef, \
    Coroutine, get_origin

from ariadne import ObjectType, QueryType

from blazelink.utils import is_optional, get_optional_arg, is_generic, resolve_refs, is_list, get_list_type, has_erased_arg, \
    FakeGeneric

_PageItem = TypeVar("_PageItem")
_QueryType = TypeVar("_QueryType")


class GenericQueryContainer(Generic[_QueryType]):
    def __init_subclass__(cls, **kwargs):
        cls._type = get_args(cls.__orig_bases__[0])[0]

    def __init__(self, query):
        self.query = query


class ScalarQuery(GenericQueryContainer):
    """ Contains a query that is expected to return single item """
    pass


class ListQuery(GenericQueryContainer):
    """ Contains a query that is expected to return list of items """
    pass


class BlazeArg:
    """ Represents GraphQL argument """

    def __init__(self, type: Type, name: str, required: bool):

        if isinstance(type, ForwardRef):
            type = type._evaluate(globals(), locals(), set())

        self.type = type
        self.name = name
        self.required = required

    def resolve(self, globs, locs):
        self.type = resolve_refs(self.type, globs, locs, recursive_guard=set())
        return self


class BlazeField:
    """ Represents GraphQL field. Field might be computed, so it has a signature. """

    def __init__(self, name: str, signature: list[BlazeArg], return_type: Type, optional: bool):

        if isinstance(return_type, ForwardRef):
            return_type = return_type._evaluate(globals(), locals(), set())

        self.name = name
        self.signature = signature
        self.return_type = return_type
        self.optional = optional


_ResolverType = Callable[['Serializable', 'BlazeProperty', 'BlazeContext', 'BlazeConfig', dict[str, Any]], Coroutine[Any, Any, Any]]
_MiddlewareType = Callable[['Serializable', Any, 'BlazeProperty', 'BlazeContext', 'BlazeConfig', dict[str, Any]], Coroutine[Any, Any, Any]]


class BlazeProperty:
    """ Represents GraphQL computed property """

    def __init__(
            self, name: str,
            getter: _ResolverType,
            signature: list[BlazeArg],
            return_type: Type,
            resolver_middleware: list[_MiddlewareType] = None
    ):
        """ Create new property

        :param name: property name
        :param getter: function that takes parent owning the field, field itself,
        config and kwargs and returns real value of the field on given parent
        :param signature: list of arguments that field accepts
        :param return_type: type of the field
        :param resolver_middleware: list of functions that take value returned by getter and return modified value.
        basically serializers
        """
        self.name = name

        if resolver_middleware:
            async def wrapped_resolver(parent: Serializable, field: BlazeProperty, context: BlazeContext, config: BlazeConfig, **kwargs):

                value = await getter(parent, field, context, config, **kwargs)

                for middleware in resolver_middleware:
                    value = middleware(parent, value, field, context, config, **kwargs)
                    if asyncio.iscoroutine(value):
                        value = await value

                return value

            self.resolver: _ResolverType = wrapped_resolver
        else:
            self.resolver = getter

        self.getter = getter
        self.signature = signature
        self.return_type = return_type


class Authenticator:
    """ Base class for GraphQL authenticators """

    def authenticate(self, request: Any) -> str:
        """ Authenticate request by returning session id
        from arbitrary source that is provided by ariadne
        and received from ASGI app.
        """
        raise NotImplementedError()


class DataAccessor:

    async def get_by_pk(self, context: 'BlazeContext', model: Type[Any], pk: Any) -> Any:
        raise NotImplementedError()

    async def execute_query(self, context: 'BlazeContext', query: GenericQueryContainer) -> Any:
        raise NotImplementedError()


T = TypeVar("T")


@dataclass
class BlazeContext(Generic[T]):
    """ Context for GraphQL query """
    user: T
    request: Any
    session_id: str
    session: Any


class BlazeConfig:
    """ Top-level configuration """

    def __init__(
            self,
            orm_class: Type[Any],
            data_accessor: DataAccessor,
            context_factory: Callable[[Any], BlazeContext]
    ):
        self.data_accessor = data_accessor
        self.orm_class = orm_class
        self.context_factory = context_factory


def _parse_signature(function) -> list[BlazeArg]:
    """ Get list of arguments from function signature. Ignores self and not annotated arguments. """

    sig = inspect.signature(function)

    params = []
    for name, param in sig.parameters.items():

        if name == "self":
            continue

        if param.annotation == inspect.Parameter.empty:
            continue

        required = param.default == inspect.Parameter.empty

        py_type = param.annotation

        if is_optional(py_type):
            py_type = get_optional_arg(py_type)
            required = False

        params.append(
            BlazeArg(
                type=py_type,
                name=name,
                required=required
            )
        )

    return params


def _get_return_type(function) -> Type:
    """ Get return type from function signature. """

    sig = inspect.signature(function)

    return_type = sig.return_annotation

    if return_type == inspect.Parameter.empty:
        raise ValueError("Computed property must have return type annotation")

    return return_type


def _instrument_computed_resolver(resolver: Callable, paginate: bool, filters: list[Callable]):
    """ Instrument resolver function for computed property with options to paginate results and apply filters
    After that, resolver will have _prop_meta attribute, containing all information about that resolver

    :param resolver: resolver function
    :param paginate: if True, will add pagination arguments to resolver
    :param filters: list of filter functions
    """

    # get args needed by raw computed prop itself
    sig = inspect.signature(resolver)

    return_type = sig.return_annotation

    if return_type == inspect.Parameter.empty:
        raise ValueError("Computed property must have return type annotation")

    param_sig = _parse_signature(resolver)

    # wrap type into page and update signature with new args
    if paginate:
        assert is_generic(return_type), f"Return type must be either list query or list, got {return_type} ({type(return_type)})"
        list_of = return_type.__args__[0]
        return_type: Type[Page] = Page[list_of]

        # page and size here
        param_sig.extend(
            return_type.get_construct_args()
        )

    # add filter props if needed
    if filters:
        for f in filters:
            filter_sig = _parse_signature(f)
            param_sig.extend(filter_sig)

    async def instrumented_getter(self, field, context: BlazeContext, config: BlazeConfig, **kwargs):
        """ New getter function that extends provided getter logic """

        # arguments that are expected by the original resolver
        resolver_args = {}

        for param in _parse_signature(resolver):
            if param.name in kwargs:
                resolver_args[param.name] = kwargs[param.name]

        # call original resolver
        resolved_value = resolver(self, **resolver_args)

        if asyncio.iscoroutine(resolved_value):
            resolved_value = await resolved_value

        if isinstance(resolved_value, GenericQueryContainer):
            resolved_value = await config.data_accessor.execute_query(context, resolved_value)

        return resolved_value

    resolver._prop_meta = BlazeProperty(
        name=resolver.__name__,
        getter=instrumented_getter,
        signature=param_sig,
        return_type=return_type
    )


def parse_class_fields(cls: Type) -> list[BlazeField]:
    fields = []
    for name, field_type in get_type_hints(cls).items():
        if name.startswith("_"):
            continue

        undefined = object()
        _is_optional = False

        value = getattr(cls, name, undefined)

        if hasattr(field_type, '__origin__') and field_type.__origin__ is Union:
            args = field_type.__args__
            if len(args) == 2 and args[1] is type(None):
                _is_optional = True
                field_type = args[0]
            if value is None:
                _is_optional = True

        fields.append(
            BlazeField(
                name=name,
                return_type=field_type,
                signature=[],
                optional=_is_optional
            )
        )

    return fields


def computed(method=None, *, paginate=False, filters: List[Callable] = None):
    """ Decorator for marking computed properties. Those will be collected and added to the schema. """

    # called as function
    if not method:
        def real_decorator(method):
            _instrument_computed_resolver(method, paginate, filters)
            return method

        return real_decorator

    # called as decorator
    _instrument_computed_resolver(method, paginate, filters)
    return method


def preprocess_field(field: BlazeProperty, cls):

    list_of_model = is_list(field.return_type) and (
        Table.issubclass(get_list_type(field.return_type)) or
        VirtualTable.issubclass(get_list_type(field.return_type))
    )

    def model_to_id_middleware(_field: BlazeProperty, is_list_: bool = False):
        async def model_to_id_resolver_wrapper(
                self: AbstractTable,
                field_value: Any,
                _field: BlazeProperty,
                context: BlazeContext,
                _config: BlazeConfig,
                **kwargs
        ):
            """ Resolver for converting model field to id.
            Wraps actual resolver and turns its return value into object id

            :param self: parent model containing this field.
            :param _field: field object that is being resolved\
            :param field_value: value of the field
            :param context: blaze context
            :param _config: additional shared config
            :param kwargs: additional gql arguments from client
            """

            # here we need different logic for converting to ObjectIds because
            # top-level entities are converted to Table objects, but resolved foreign keys
            # are retrieved from actual database model, meaning UserTableInstance.team will be
            # Team model instead of TeamTable. May change in the future, possible solution is to
            # extend __getattribute__ logic to convert all foreign keys to Table objects

            # make field always be a list to simplify logic
            if not is_list_:
                field_value = [field_value]

            items = []
            for item in field_value:
                if item is None:
                    items.append(None)

                # convert table object to ObjectId
                elif isinstance(item, (VirtualTable, Table)):
                    raise ValueError("If this ever happens, I need to add __dependencies__ to returned object id")
                    items.append(item.get_unique_id())
                else:
                    if not hasattr(item, "id"):
                        raise ValueError(f"Object {item} doesn't have id attribute")

                    table = Table.get_by_orm_model(type(item))

                    assert self.context is not None
                    deps = table(item, self.context).__dependencies__()

                    # convert database object to ObjectId
                    items.append(
                        ObjectId(
                            obj_id=item.id,
                            entity=type(item).__name__,
                            dependencies=deps
                        )
                    )

            if is_list_:
                return items

            # if field wasn't a list, turn back to single item
            assert len(items) == 1
            return items[0]

        return model_to_id_resolver_wrapper

    # Field is a (list of) Table/VirtualTable type
    if Table.issubclass(field.return_type) or VirtualTable.issubclass(field.return_type) or list_of_model:
        prop = BlazeProperty(
            name=field.name,
            getter=field.getter,
            resolver_middleware=[model_to_id_middleware(field, is_list_=list_of_model)],
            signature=[],
            return_type=field.return_type
        )

        return resolve_erased_args_of_generic(prop, cls)

    # Computed fields need resolvers, because they have args
    if ComputedField.issubclass(field.return_type):
        computed_field: Type[ComputedField] = field.return_type

        # computed fields add args from initial resolver with arguments from computed field.
        # firstly we need a set of args to resolve a field, and then we need args to resolve
        # computed field from previously resolved field.
        og_prop = BlazeProperty(
            name=field.name,
            getter=field.getter,
            signature=field.signature + computed_field.get_construct_args(),
            return_type=field.return_type,
            resolver_middleware=[computed_field.class_based_prop_middleware]
        )

        return resolve_erased_args_of_generic(og_prop, cls)

    return field


class Serializable:
    """ Base abstract class in the table hierarchy. """

    _class_generics: dict[str, Type] = {}

    # static registry of computed props by name on this type
    _computed_props: dict[str, list[BlazeProperty]] = defaultdict(list)

    # table manager which registered this type
    _table_mgr: 'TableManager' = None

    _field_cache: dict[Type['Serializable'], dict[str, BlazeProperty]] = {}

    @classmethod
    def get_type_name(cls):
        """ Get GraphQL name of this type, such as 'Player' """
        raise NotImplementedError

    @classmethod
    def get_construct_args(cls) -> List[BlazeArg]:
        """ List of constructor arguments for this type """
        raise NotImplementedError

    @classmethod
    def get_generic_arg(cls, var: TypeVar):
        return cls._class_generics.get(var.__name__)

    @classmethod
    def set_generic_arg(cls, var: TypeVar, value: Type):
        cls._class_generics[var.__name__] = value

    @classmethod
    def collect_fields(cls, model_list) -> dict[str, BlazeProperty]:
        """ Collect all fields that are exposed by the model """

        if cls in cls._field_cache:
            return cls._field_cache[cls]

        _fields: dict[str, BlazeProperty] = {}

        globs = globals()

        # add models to globals
        for model in model_list:
            globs[model.__name__] = model

        # add props registered with @computed decorator
        for prop in cls._computed_props[cls.__name__]:
            prop = resolve_erased_args_of_generic(prop, cls)
            prop.return_type = resolve_refs(prop.return_type, locals(), globs, set())

            _fields[prop.name] = prop

        for field in parse_class_fields(cls):

            if field.name in _fields:
                continue

            # freshly parsed fields are not resolved yet
            field = resolve_erased_args_of_generic(field, cls)

            async def attribute_resolver(self: Serializable, f: BlazeProperty, context: BlazeContext, config: BlazeConfig, **kwargs):
                if isinstance(self, Table):
                    return getattr(self.get_model(), f.name)
                return getattr(self, f.name)

            field = resolve_erased_args_of_generic(field, cls)

            _fields[field.name] = BlazeProperty(
                name=field.name,
                getter=attribute_resolver,
                signature=field.signature,
                return_type=field.return_type
            )

        # process all props to do stuff like replacing models into
        # object ids and apply custom resolvers
        for name, prop in _fields.items():

            # resolve signature
            prop.signature = [
                arg.resolve(locals(), globs) for arg in prop.signature
            ]

            prop.return_type = resolve_refs(prop.return_type, locals(), globals(), set())

            _fields[name] = preprocess_field(prop, cls)

        cls._field_cache[cls] = _fields

        return cls._field_cache[cls]

    @classmethod
    def get_model_props(cls, model_list: List[Type['Serializable']]) -> List[BlazeProperty]:
        """ Parse field list from this type """

        fields = cls.collect_fields(model_list)

        return list(fields.values())

    @classmethod
    async def resolve(
            cls,
            __parent: Optional['AbstractTable'],
            __field: Optional[BlazeField],
            __context: BlazeContext,
            __config: BlazeConfig,
            **extra_args
    ):
        """ Resolve upper level of model without any recursion

            :param cls: the class of the model to resolve
            :param __parent: the parent model value. Not necessarily an instance of cls.
            :param __field: the field of the parent model that contains this model
            :param __context: the context of the request
            :param __config: additional shared configurations
            :param extra_args: additional arguments passed to the resolver

            :return: the resolved model
        """
        raise NotImplementedError

    @classmethod
    async def resolve_with_context(cls, __info, __config: BlazeConfig, __context: BlazeContext, **fields) -> tuple['Serializable', BlazeContext]:
        """ Called when ariadne is resolving a query for this type.
        Always a root resolver for queryable types.

        :param __info: ariadne info object
        :param __config: additional configuration
        :param fields: query arguments

        :returns: instance of this type
        """



        # __context = {
        #     'request': request,
        #     'session_id': session_id
        # }


        obj = await cls.resolve(None, None, __context, __config, **fields)

        return obj

    @classmethod
    def issubclass(cls, model_type):
        return (isinstance(model_type, type) and issubclass(model_type, cls)) or (
            is_generic(model_type) and issubclass(get_origin(model_type), cls)
        )


class AbstractTable(Serializable):

    @classmethod
    def getter_name(cls):
        """ Get name of getter for objects of this type.
        where getter is a top level query function like this:
        player(id: Int!): Player
        """
        return cls.__name__[0].lower() + cls.__name__[1:]

    def get_unique_id(self) -> 'ObjectId':
        raise NotImplementedError

    def __dependencies__(self):
        """ Get a list of primitives this model depends on. """
        return []

    def to_dict(self):
        raise NotImplementedError


T = TypeVar("T")


def resolve_erased_args_of_generic(field: BlazeField | BlazeProperty, field_owner: Type[Serializable]):
    if is_generic(field.return_type) and has_erased_arg(field.return_type):

        generic_arg = field_owner.get_generic_arg(field.return_type.__args__[0])
        assert generic_arg is not None, f"Generic arg {field.return_type.__args__[0]} is not set"
        field.return_type = FakeGeneric(
            field.return_type,
            origin=field.return_type.__origin__,
            args=(generic_arg,)
        )

    for arg in field.signature:
        if is_generic(arg.type) and has_erased_arg(arg.type):
            generic_arg = field_owner.get_generic_arg(arg.type.__args__[0])
            assert generic_arg is not None, f"Generic arg {field.return_type.__args__[0]} is not set"
            arg.type = FakeGeneric(
                field.return_type,
                origin=field.return_type.__origin__,
                args=(generic_arg,)
            )

    return field


class Table(Generic[T], AbstractTable):
    """ Base class for physical table representation. """

    _model_type: Type[T]
    _model_by_table: dict[str, Type['Table[T]']] = {}

    def __init__(self, model, context: BlazeContext):

        if model is None:
            raise ValueError("Model cannot be None in table constructor")

        self.__model = model
        self.context: BlazeContext = context

        # stores recursively resolved value of this model,
        # that matches the request
        self._resolved_values = {}

    def get_model(self):
        return self.__model

    @classmethod
    def get_by_orm_model(cls, model: Type[T]) -> Type['Table[T]']:
        return cls._model_by_table[model.__name__]

    def __init_subclass__(cls) -> None:
        # save model type
        cls._model_type = get_args(cls.__orig_bases__[0])[0]
        # map database orm model to blaze link table
        cls._model_by_table[cls._model_type.__name__] = cls

    @classmethod
    def get_construct_args(cls) -> List[BlazeArg]:
        # database table row has only one arg - id
        return [BlazeArg(
            type=ObjectId,
            name="identifier",
            required=True
        )]

    def to_dict(self):
        return self._resolved_values

    @classmethod
    async def resolve(
            cls,
            __parent,
            __field: Optional[BlazeField],
            __context: BlazeContext,
            __config: BlazeConfig,
            **args
    ):
        """ Resolve database table row with fields passed from GraphQL. Always receives ObjectID dictionary as args. """

        model = await __config.data_accessor.get_by_pk(__context, model=cls.get_model_type(), pk=args['identifier'].obj_id)

        table = cls(model, __context)

        return table

    @classmethod
    def get_model_type(cls):
        return cls._model_type

    @classmethod
    def get_type_name(cls):
        return cls._model_type.__name__

    @classmethod
    def getter_name(cls):
        n = cls.get_type_name()[0].lower() + cls.get_type_name()[1:]
        return n

    def get_unique_id(self) -> 'ObjectId':
        return ObjectId(
            obj_id=self.get_model().id,
            entity=self.get_type_name(),
            dependencies=[],
        )


class VirtualTable(AbstractTable):
    """
        Virtual tables are tables that are not stored in the database, but are instead generated from other tables.
        They can have similar interface to a normal table, but they might have multiple primary keys defining them.

        The main difference between a virtual table and a normal table is that virtual tables build their requests
        dynamically, based on init arguments. They are initialized through constructor.
    """

    @classmethod
    def get_construct_args(cls) -> List[BlazeArg]:
        # virtual table, just like table, is initialized with id
        return [BlazeArg(
            type=ObjectId,
            name="identifier",
            required=True
        )]

    @classmethod
    def get_type_name(cls):
        return cls.__name__

    @classmethod
    async def resolve(cls,  __parent, __field: Optional[BlazeField], __context: BlazeContext, __config: BlazeConfig, **extra_args):
        identifier = extra_args.get("identifier")

        # no idea what this was
        # identifier = ObjectId.find(
        #     obj_id=extra_args.get('obj_id'),
        #     entity=cls.get_type_name(),
        #     dependencies=extra_args.get('dependencies', []),
        #     increase_ref_count=True,
        # )

        # todo: pass context somehow?
        inst = cls(identifier, __context)
        inst.context = __context
        inst.identifier = identifier
        return inst

    def get_unique_id(self) -> 'ObjectId':
        return self.identifier


class Struct(AbstractTable):
    """ Struct is a virtual table that is initialized with a dictionary of values,
    unlike virtual tables, which are initialized with ObjectId.
    """
    @classmethod
    def get_construct_args(cls) -> List[BlazeArg]:
        return _parse_signature(cls.__init__)

    @classmethod
    def get_type_name(cls):
        return cls.__name__

    @classmethod
    async def resolve(cls, __parent, __field: Optional[BlazeField], __context: BlazeContext, __config: BlazeConfig, **kwargs):
        # todo: pass context somehow?
        struct = cls(**kwargs)
        struct.context = __context
        return struct

    def __init__(self, **kwargs):
        self._values = kwargs

    def to_dict(self):
        return self._values


class GenericModel:
    pass


X = TypeVar("X")


class ObjectId(Struct):
    """ Universal identifier for any object.
    It can identify row inside physical table as well as virtual object that has dependencies. """

    obj_id: int
    entity: str
    modifiers: str
    dependencies: list[ForwardRef('ObjectId')]

    def __init__(
            self,
            entity: str,
            obj_id: int = None,
            modifiers: str = None,
            dependencies: list[ForwardRef('ObjectId')] = None
    ):
        """ Create new ObjectId instance

        :param obj_id: numerical id of the object from physical table
        :param entity: name of the entity
        :param modifiers: dictionary of arbitrary arguments that define how data is preprocessed,
        for example it may define ordering. Passed around as JSON string
        :param dependencies: list of dependencies
        """

        self.obj_id: int = obj_id
        self.entity: str = entity

        if modifiers is None:
            modifiers = "[]"

        self.modifiers = modifiers
        self._parsed_modifiers = json.loads(modifiers)

        if not dependencies:
            dependencies = []

        self.dependencies: list[ObjectId] = dependencies

    def same_entity(self, other: 'ObjectId'):
        return self.entity.lower().replace("_", "") == other.entity.lower().replace("_", "")

    def depends_on(self, identifier):
        """ Check if this object id depends on given object id """

        if self == identifier:
            return True

        # entities match, subscribed to entire table
        if self.obj_id is None and self.same_entity(identifier):
            return True

        return False

    def find_dependency(self, name) -> Optional['ObjectId']:
        """ Find dependency by name """
        for dep in self.dependencies:
            if dep.entity.lower().replace("_", "") == name.lower().replace("_", ""):
                return dep

    def __eq__(self, other):
        """ Check if two ObjectIds are equal.
        They are equal if they have the same id, entity and dependencies (order ignored)
        """

        ent1 = self.entity.lower().replace("_", "")
        ent2 = other.entity.lower().replace("_", "")

        return self.obj_id == other.obj_id and \
            ent1 == ent2 and \
            set(self.dependencies) == set(other.dependencies)

    def __hash__(self):
        res = 0
        for dep in self.dependencies:
            res ^= dep.__hash__()

        return f"{self.obj_id}-{self.entity}-{res}".__hash__()

    def __str__(self):
        return f"<ObjectId id={self.obj_id} ent={self.entity} deps={self.dependencies}>"

    def __repr__(self):
        return self.__str__()

    def get_modifiers(self) -> dict[str, Any]:
        return self._parsed_modifiers

    def dict(self, minimal=False):
        if minimal:
            return {
                "obj_id": self.obj_id,
                "entity": self.entity,
            }
        return {
            "obj_id": self.obj_id,
            "entity": self.entity,
            "modifiers": self.modifiers,
            "dependencies": [dep.dict() for dep in self.dependencies]
        }

    @classmethod
    def load(cls, data: dict) -> 'ObjectId':
        """ Load ObjectId from dictionary recursively, or return existing instance """

        deps = [
            ObjectId.load(dep) for dep in data.get('dependencies', [])
        ]
        new = ObjectId(
            obj_id=data.get('obj_id'),
            entity=data.get('entity'),
            dependencies=deps
        )
        return new


class ComputedField(Serializable):
    """ Represents a special field type that depends on another field of that model and has to be resolved
    using additional arguments. Basically computed table, but a field.

    model_field(a: Int b: Int) -> RetType

    Class extending ComputedField must implement __init__ with (self, value, [**kwargs]) signature,
    where self if the field object, value is the value of the field that it depends on and kwargs are
    additional arguments passed to the resolver. Each of the kwargs must be annotated.
    Return type of this class is the type of the field.

    You can think of computed field as an extension of a regular field, allowing to chain logic of initial resolver
    defined on model with additional logic from computed field. This means that arguments required on actual
    graphql resolver would be a union of arguments required by model field and computed field.
    """

    @classmethod
    async def class_based_prop_middleware(
            cls,
            parent: Serializable,
            field_value: Any,
            field: BlazeProperty,
            __context: BlazeContext,
            config: BlazeConfig,
            **extra_args
    ):
        """ This method will be used to turn field real value into a serialized field. This method follows middleware
        signature and will be used as middleware.
        """

        # filter only fields expected by __init__
        extra_args = {
            k: v for k, v in extra_args.items()
            if k in [arg.name for arg in cls.get_construct_args()]
        }

        # Class extending ComputedField must implement __init__ with (self, value, [**kwargs]) signature
        # todo: pass context comehow?
        inst = cls(field_value, **extra_args)
        inst.context = __context
        return inst

    @classmethod
    def get_type_name(cls):
        return cls.__name__

    @classmethod
    def get_construct_args(cls) -> list[BlazeArg]:
        return _parse_signature(cls.__init__)


class Page(Generic[_PageItem], ComputedField):

    # page items
    items: list[_PageItem]
    # total item count
    count: int

    def __init_subclass__(cls) -> None:
        # save generic arg here, before it gets erased
        arg = get_args(cls.__orig_bases__[0])[0]

        if isinstance(arg, TypeVar):
            raise ValueError("Generic class initialized without argument")

        cls.set_generic_arg(_PageItem, arg)

    def __init__(self, value, page: int = 0, size: int = 10):
        print("Page", page, "size", size)
        items = value[size * page:size * (page + 1)]
        self.items = items
        self.count = len(value)


class TableManager:
    """ Central class of the library responsible for registration of schemas and their resolvers. """

    PYTHON_TO_GQL = {
        bool: "Boolean",
        int: "Int",
        str: "String",
        float: "Float",
        datetime: "String"
    }

    def __init__(self, config: BlazeConfig):
        self.models: List[Type[AbstractTable]] = []
        self.queryable: List[Type[AbstractTable]] = []
        self.gql_objects: List[ObjectType] = []
        self.config = config

        # list of models by name that have to be converted
        # to string type representation
        self.models_to_process: dict[str, Type[AbstractTable]] = {}
        self._on_subscribe_callbacks = []

        # register internal types
        self._register_type(ObjectId, queryable=False)

    def on_subscribe(self, callback):
        self._on_subscribe_callbacks.append(callback)
        return callback

    def is_orm_class(self, cls: Type) -> bool:
        return isinstance(cls, type) and issubclass(cls, self.config.orm_class)

    def get_gql_objects(self):
        """ GraphQL types that will be added to the schema """
        return self.gql_objects

    def _register_type(self, type: Type[AbstractTable], queryable: bool) -> bool:
        """ Register GraphQL type if it wasn't already """

        # do not register twice
        if type in self.models:
            return False

        # link each registered type to the manager
        type._table_mgr = self

        self.models.append(type)
        self.models_to_process[type.__name__] = type

        if queryable:
            self.queryable.append(type)

        # go through all methods and find computed properties
        # that need to be registered
        for name in dir(type):
            method = getattr(type, name)

            if not callable(method):
                continue

            if not hasattr(method, '_prop_meta'):
                continue

            prop: BlazeProperty = method._prop_meta

            # add property to static list of computed properties
            type._computed_props[type.__name__].append(prop)

        return True

    def table(self, func_or_class=None, **kwargs):
        """ Decorator for registering GraphQL table type. """

        if func_or_class is None:
            def wrapper(cls):
                self._register_type(cls, kwargs.get("queryable", True))
                return cls

            return wrapper
        else:
            self._register_type(func_or_class, True)
            return func_or_class

    def type(self, cls: Type[AbstractTable]):
        """ Decorator for registering GraphQL type. Type is not queryable.
            Can ge generic, which means multiple GraphQL types will be created.
        """
        self._register_type(cls, False)
        return cls

    def create_graphql_request(self, table: Type[AbstractTable]) -> str:
        """ Creates string request representation to query object of this type.
        example: player(id: Int!): Player
        """

        name = table.getter_name()

        result = f"{name}"

        args = table.get_construct_args()

        compiled_args = ""

        for i, arg in enumerate(args):

            is_last = i == len(args) - 1
            comma = "," if not is_last else ""
            # todo: add support for custom types
            try:
                _mapped = self.py_to_gql(arg.type, input=True)
            except KeyError:
                raise ValueError(f"Unknown type {arg.type} for field {arg.name} in {table.__name__}")
            compiled_args += f"{arg.name}: {_mapped}{comma}"

        if compiled_args:
            result += f"({compiled_args})"

        result += ": " + table.get_type_name()
        return result

    def get_graphql_requests(self) -> str:
        """ Construct all possible GraphQL requests for existing queryable tables.
        Requests are constructed from table constructors.

        :return: GraphQL requests definition string
        """
        requests = []
        for model in self.queryable:
            try:
                requests.append("    " + self.create_graphql_request(model))
            except Exception as e:
                print(f"Error while generating graphql request for {model.__name__}")
                raise e

        return "\n".join(requests)

    def create_graphql_response(self, cls):
        """
            Creates GraphQl definition of model type.
            Includes all fields and computed properties.

            type Player { username elo team_id }
        """

        type_definition = f"type {cls.get_type_name()} {{\n"

        model_field: BlazeField
        for model_field in cls.get_model_props(self.models):

            if inspect.isclass(model_field.return_type) and self.is_orm_class(model_field.return_type):
                raise ValueError("Model in graphql object, use id instead!")

            # make sure model fields are registered as well
            if AbstractTable.issubclass(model_field.return_type):
                self._register_type(model_field.return_type, queryable=False)

            field_args = ""

            if Struct.issubclass(model_field.return_type):
                pass

            # some fields may actually be functions
            if model_field.signature:
                field_args = "(" + ", ".join(
                    [f"{arg.name}: {self.py_to_gql(arg.type, field_owner=cls)}" for arg in model_field.signature]) + ")"

            # type of the field in graphql
            if Table.issubclass(model_field.return_type) or VirtualTable.issubclass(model_field.return_type):
                # we swap models for ObjectIds here
                graphql_field = ObjectId.get_type_name()
            else:
                graphql_field = self.py_to_gql(model_field.return_type, field_owner=cls)

            assert graphql_field is not None, f"Type {model_field.return_type} for field {model_field.name} in {cls.__name__} was not compiled"

            # player(args): ObjectId
            type_body_row = f"    {model_field.name}{field_args}: {graphql_field}\n"
            type_definition += type_body_row

        type_definition += "}\n"

        return type_definition

    def get_graphql_responses(self) -> str:
        """ Construct all possible GraphQL responses for existing queryable tables.
        Responses are constructed from table fields.

        :return GraphQL type definition string
        """

        typedefs = ""
        compiled = set()
        # avoid concurrent modification which can happen when
        # during model compilation new models are discovered
        while self.models_to_process:
            key = self.models_to_process.keys().__iter__().__next__()

            if key in compiled:
                continue

            compiled.add(key)

            model = self.models_to_process.pop(key)
            typedefs += self.create_graphql_response(model)

        return typedefs

    def create_graphql_inputs(self, cls: Type[AbstractTable]) -> str:
        """ Create GraphQL input type for table constructor. All inputs have 'Input' suffix. """

        result = f"input {cls.get_type_name()}Input {{\n"

        for field in cls.get_construct_args():
            result += f"    {field.name}: {self.py_to_gql(field.type, input=True)}\n"

        result += "}\n"
        return result

    def get_graphql_inputs(self) -> str:
        """ Collect all input types for all queryable tables and their properties and compile it """

        inputs: set[Type[AbstractTable]] = set()

        for model in self.models:
            for arg in model.get_construct_args():

                if isinstance(arg.type, str):
                    arg.type = eval(arg.type)

                if issubclass(arg.type, AbstractTable):
                    inputs.add(arg.type)

            for prop in model.get_model_props(self.models):
                for arg in prop.signature:
                    if issubclass(arg.type, AbstractTable):
                        inputs.add(arg.type)

        result = ""

        for inp in inputs:
            result += self.create_graphql_inputs(inp)

        return result

    def define_resolvers(self, query: QueryType):
        """ Add resolvers for all queryable types to the query type object. """

        # Oh, no! python's closures are broken!
        def make_getter_resolver(model_type: Type[AbstractTable]):
            """ This function will create resolvers from top level models defined on query. """

            @query.field(model_type.getter_name())
            async def query_item_resolver(_, info, **fields):

                resolving_by_id: bool = False
                if 'identifier' in fields:
                    resolving_by_id = True

                    data = fields['identifier']

                    # make sure entry on requested object is set and valid
                    if data.get('entity') is not None:
                        assert data['entity'] == model_type.get_type_name(), f"Invalid entity type! Expected: {model_type.get_type_name()}, got {data['entity']}"
                    else:
                        data['entity'] = model_type.get_type_name()

                    # this is the place where input data is resolved into ObjectId
                    # should be the only place too
                    obj_id = ObjectId.load(data)

                    # parse object id and replace raw data with it.
                    # do we want to only have object id as argument type always?
                    fields['identifier'] = obj_id

                context = self.config.context_factory(info)

                resolved = await model_type.resolve_with_context(
                    info,
                    self.config,
                    context,
                    **fields
                )

                # Now we can call callbacks that listen to new model subscriptions
                if resolving_by_id:
                    for callback in self._on_subscribe_callbacks:
                        await callback(context.session_id, fields['identifier'])

                return resolved

        def make_computed_prop_resolver(gql_obj, b_prop: BlazeProperty):

            @gql_obj.field(b_prop.name)
            async def test(obj, info, **kwargs):
                # unlike model resolves, here we don't have resolve_with_context method,
                # so we have to kind of duplicate that to make sure context is passed to the resolver

                context = self.config.context_factory(info)
                res = b_prop.resolver(obj, b_prop, context, self.config, **kwargs)
                if asyncio.iscoroutine(res):
                    res = await res
                return res

        for model in self.models:
            if model in self.queryable:
                make_getter_resolver(model)

            gql_obj = ObjectType(model.get_type_name())
            self.gql_objects.append(gql_obj)

            for prop in model.get_model_props(self.models):
                make_computed_prop_resolver(gql_obj, prop)

    def py_to_gql(self, py_type: type, input=False, field_owner=None) -> str:
        """ Converts a python type to a graphql type.
        Does replace nested models with ObjectIds.

        :param input: Whether to convert to input type (adding prefix)
        :param field_owner: reference to root type, I don't exactly remember what it does

        :returns: GraphQL string representation of the python type
        """

        if hasattr(py_type, '__origin__'):
            return self.generic_type_to_gql(py_type, input=input, field_owner=field_owner)

        if isinstance(py_type, type) and issubclass(py_type, AbstractTable):
            type_name = py_type.get_type_name()
            if input:
                return type_name + "Input"

            return type_name

        for k, v in self.PYTHON_TO_GQL.items():
            if isinstance(py_type, type) and issubclass(py_type, k):
                return v

        if self.is_orm_class(py_type):
            raise ValueError("ORM class detected. Use Table type instead")

        raise ValueError(f"Can't convert type {py_type} ({type(py_type)}) to GraphQL type.")

    def generic_type_to_gql(self, py_type, input=False, field_owner=None):
        """ Convert generic python type to GraphQL type."""

        origin = py_type.__origin__
        generic_args = py_type.__args__

        # no idea what is the point of using first union argument
        # I think it is for optionals
        if origin == Union:
            raise ValueError("Is this used?")
            # return py_to_gql(generic_args[0], field_owner=field_owner)

        if issubclass(origin, (GenericModel, ComputedField)):
            # we have a parametrized custom type.

            # we detected a custom type, so we need to generate a helper table.
            # example: Page[CustomIdType]

            helper_type_name = self.generate_helper_table_name(origin, generic_args)

            print(f"craete helper type from", origin, generic_args)
            class HelperType(origin[generic_args[0]]):
                # TODO: store a dictionary of generic vars here instead of single var
                #  to support multiple generic vars, like Page[CustomIdType, Int32]
                _generic_type = generic_args[0]
                __args__ = generic_args

            HelperType.__name__ = helper_type_name
            print("created generic helper", HelperType)
            self._register_type(HelperType, False)

            if input:
                return helper_type_name + "Input"

            return helper_type_name

        if origin and issubclass(origin, AbstractTable):
            type_name = origin.get_type_name()

            if input:
                return type_name + "Input"

            return type_name

        if origin is list:
            return f"[{self.list_item_type_to_gql(generic_args, input, field_owner)}]"

        raise ValueError(f"Can't convert type {py_type} to GraphQL type. This generic type is not supported.")

    @classmethod
    def generate_helper_table_name(cls, base, generic_args):
        return f"GenericHelper__{base.__name__}__{generic_args[0].__name__}"

    def list_item_type_to_gql(self, generic_args, input=False, field_owner=None):
        """
            Formats contents of generic type. List items are always presented as primitives.
            List[Player] will become Player_id, List[int] will become Int, etc.

            :param generic_args: generic type arguments
            :param input: whether field is input field and needs Input suffix
            :param field_owner: type that owns this field.
            Used for generic types resolution at runtime
        """

        generic_arg = generic_args[0]

        # if forward ref update
        if isinstance(generic_arg, ForwardRef):
            generic_arg = generic_arg._evaluate(globals(), locals(), recursive_guard=set())

        # type vars are not resolved at runtime unfortunately,
        # so we need to get generic type from the field owner
        if isinstance(generic_arg, TypeVar):
            # TODO: get generic var type by name from dictionary
            generic_arg = field_owner._generic_type

        if isinstance(generic_arg, list):
            raise ValueError("List inside list")

        if generic_arg in self.PYTHON_TO_GQL:
            return self.PYTHON_TO_GQL[generic_arg]

        # database model field
        if self.is_orm_class(generic_arg):
            raise ValueError("ORM field detected. Use Table type instead")

        if type(generic_arg) == str:
            raise ValueError("Unresolved ForwardRef")

        # virtual table field, will have dependency list
        if issubclass(generic_arg, Table | VirtualTable):
            type_name = ObjectId.get_type_name()

            if input:
                return type_name + "Input"

            return type_name

        if Struct.issubclass(generic_arg):
            # todo: figure out how exactly field owner works ffs
            return self.py_to_gql(generic_arg, input=input, field_owner=None)

        raise ValueError(f"Unknown type {generic_arg} {type(generic_arg)}")
