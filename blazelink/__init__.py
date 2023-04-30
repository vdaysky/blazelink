from ariadne import make_executable_schema, QueryType

from blazelink.models import TableManager, Authenticator


def create_schema(table_manager: TableManager):
    """ Create GraphQL schema from table manager """

    type_defs = f"{table_manager.get_graphql_responses()}" \
                f"{table_manager.get_graphql_inputs()}" \
                f"type Query {{\n" \
                f"{table_manager.get_graphql_requests()}" \
                f"\n}}"

    query = QueryType()
    table_manager.define_resolvers(query)

    print(type_defs, table_manager.get_gql_objects())

    return make_executable_schema(
        type_defs,
        query,
        *table_manager.get_gql_objects()
    )
