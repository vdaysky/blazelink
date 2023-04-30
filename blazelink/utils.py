from typing import Any, Union, ForwardRef, TypeVar, _GenericAlias


def is_union(x: Any) -> bool:
    return hasattr(x, "__origin__") and x.__origin__ is Union


def is_generic(x: Any) -> bool:
    return hasattr(x, "__origin__")


def has_erased_arg(x: Any) -> bool:
    assert len(x.__args__) == 1, "Multiple generic args not supported"
    # generic arg passed from Generic[T] to class items will be typewar
    return isinstance(x.__args__[0], TypeVar)


def is_list(x: Any) -> bool:
    return is_generic(x) and x.__origin__ is list


def get_list_type(x: Any) -> Any:
    return x.__args__[0]


def FakeGeneric(type, origin, args):

    print("Create fake generic for ", type, origin, args)
    return _GenericAlias(
        origin,
        tuple(args)
    )


def resolve_refs(type, locs, globs, recursive_guard):

    if isinstance(type, ForwardRef):
        return type._evaluate(locs, globs)

    if is_generic(type):
        args = type.__args__

        updated = []
        for arg in args:
            if is_generic(arg):
                arg = resolve_refs(arg, globs, locs, recursive_guard=recursive_guard)
            if isinstance(arg, ForwardRef):
                arg = arg._evaluate(locs, globs, recursive_guard=recursive_guard)
            updated.append(arg)

        return FakeGeneric(type, type.__origin__, updated)

    if isinstance(type, str):
        return eval(type, globs, locs)

    return type


def is_optional(x: Any) -> bool:
    return is_union(x) and len(x.__args__) == 2 and x.__args__[1] is type(None)


def get_optional_arg(x: Any) -> Any:
    return x.__args__[0]


def get_generic_args(x: Any) -> list:
    return x.__args__
