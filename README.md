# Blazelink
## WIP

# Notes:

`resolve` function is called when graphql request is received. for tables, it passes arguments to filter function.

for virtual tables, it passes arguments to constructor.

Arguments for virtual types are inferred from constructor.
Arguments for tables are always the same, id: int

## flow:

request comes in, object id is formed from arguments, table class gets resolved,
table instance returned to ariadne. Ariadne uses registered resolvers and takes fields from instance itself.

| Action         | Table                                              | Virtual Table                                                                                              | Struct                                                                                              |
|----------------|----------------------------------------------------|------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------|
| Initialization | From ObjectId argument, which only contains obj_id | From ObjectId, does not have obj_id but have dependencies. Initialization logic implemented in constructor | From any arguments, constructor signature used to get argument list, constructor used to initialize |