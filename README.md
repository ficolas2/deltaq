# deltaq

A simple terminal client for working with [Delta Lake](https://delta.io) tables.  

## Usage
Start the REPL, and type the commands. Commands start with ".", anything not starting with a dot is treated like an SQL statement.

## Open tables
Use `.open` to connect a logical name to a Delta table in a folder, or stored in S3 or compatible.
```bash
.open [OPTIONS] <TABLE_NAME> <TABLE_PATH>
```
- `<TABLE_NAME>`  Logical name you want to assign
- `<TABLE_PATH>`  s3://bucket/path
Options:
- `--endpoint-url`: http://host:port for S3 endpoint
- `--access-key-id`: Access key
- `--secret-access-key`: Secret key
- `--allow-http`: true/false [default: true]
- `--addressing-style`: S3 addressing style: path/virtual [default: "path"]
- `--conditional-put`: Conditional write mechanism: etag/dynamodb [default: "etag"]

Example:
```bash
deltaq> .open table s3://test/table \
  --endpoint-url http://localhost:9000 \
  --access-key-id minio \
  --secret-access-key minio12345 \
  --allow-http true
```

## Create
Create a table with a given schema
```bash
.create --schema 'SCHEMA' <TABLE_NAME> <TABLE_PATH>
```
A schema is a comma-separated list of fields:
`name: type[?], other: type2[?], ...`

- `?` marks the field or element as nullable.
- Supported primitive types: `string`, `long`, `int`, `short`, `byte`, `float`, `double`, `boolean`, `binary`, `date`, `timestamp`.
- Arrays: `array<element_type[?]>`
- Structs: `struct<field1: type1[?], field2: type2[?], ...>`
- Maps: `map<key_type, value_type[?]>`

## Show open tables
Displays the list of opened tables
```bash
.tables
```

## Show schema
Prints the schema for an opened table
```bash
.schema <TABLE_NAME>
```

# TODO
- Arguments to start the REPL with tables already loaded
- Write data
- Time travel
- Checkpoints
- Read metadata
