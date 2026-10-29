# wrench

`wrench` is a schema management tool for [Cloud Spanner](https://cloud.google.com/spanner/).

Please feel free to report issues and send pull requests, but note that this
application is not officially supported as part of the Cloud Spanner product.

```sh
$ cat ./_examples/schema.sql
CREATE TABLE Singers (
  SingerID STRING(36) NOT NULL,
  FirstName STRING(1024),
) PRIMARY KEY(SingerID);

# create database with ./_examples/schema.sql
$ wrench create --directory ./_examples

# create migration file
$ wrench migrate create --directory ./_examples
_examples/migrations/000001.sql is created

# edit _examples/migrations/000001.sql
$ cat ./_examples/migrations/000001.sql
ALTER TABLE Singers ADD COLUMN LastName STRING(1024);

# execute migration
$ wrench migrate up --directory ./_examples

# load ddl from database to file ./_examples/schema.sql
$ wrench load --directory ./_examples

# finally, we have successfully migrated database!
$ cat ./_examples/schema.sql
CREATE TABLE SchemaMigrations (
  Version INT64 NOT NULL,
  Dirty BOOL NOT NULL,
) PRIMARY KEY(Version);

CREATE TABLE Singers (
  SingerID STRING(36) NOT NULL,
  FirstName STRING(1024),
  LastName STRING(1024),
) PRIMARY KEY(SingerID);
```

## Installation

Get binary from [release page](https://github.com/cloudspannerecosystem/wrench/releases).
Or, you can use [Docker image from packages.](https://github.com/cloudspannerecosystem/wrench/pkgs/container/wrench)

## Usage

### Prerequisite

```sh
export SPANNER_PROJECT_ID=your-project-id
export SPANNER_INSTANCE_ID=your-instance-id
export SPANNER_DATABASE_ID=your-database-id
```

You can also specify project id, instance id and database id by passing them as command arguments.

### Credentials

Wrench uses Application Default Credentials when `--credentials_file` is omitted. To use a JSON
credentials file, pass `--credentials_file`. The `--credentials_type` flag tells Wrench which JSON
format to expect and defaults to `service_account`.

Use the default for a service account key:

```sh
$ wrench --credentials_file=/path/to/service-account.json create --directory ./_examples
```

Use `authorized_user` for a user credentials file that contains a refresh token:

```sh
$ wrench --credentials_file=/path/to/user-credentials.json --credentials_type=authorized_user create --directory ./_examples
```

Wrench accepts `service_account` and `authorized_user`. It returns an error for other credential
types. When `--credentials_file` is omitted, `--credentials_type` has no effect and Wrench uses
Application Default Credentials.

Choosing a credential type does not validate a credentials file from an untrusted source. Follow
Google's [credential configuration validation guidance](https://cloud.google.com/docs/authentication/external/externally-sourced-credentials)
before using externally supplied credential files.

### Create database

```sh
$ wrench create --directory ./_examples
```

This creates the database with `./_examples/schema.sql`.

### Drop database

```sh
$ wrench drop
```

This just drops the database.

Pass `-i`/`--interactive` to be shown the target database and asked to confirm first.
Without it, nothing is asked. `reset` and `truncate` accept the same flag.

### Reset database

```sh
wrench reset --directory ./_examples
```

This drops the database and then re-creates with `./_examples/schema.sql`. Equivalent to `drop` and then `create`.

### Load schema from database to file

```sh
$ wrench load --directory ./_examples
```

This loads schema DDL from database and writes it to `./_examples/schema.sql`.

### Create migration file

```sh
$ wrench migrate create --directory ./_examples
```

This creates a next migration file like `_examples/migrations/000001.sql`. You will write your own migration DDL to this file.

### Execute migrations

```sh
$ wrench migrate up --directory ./_examples
```

This executes migrations. This also creates `SchemaMigrations` table into your database to manage schema version if it does not exist.

### Use custom migration table

By default, wrench uses `SchemaMigrations` table to manage migration versions. You can specify a custom table name using `--migration_table_name` flag:

```sh
$ wrench migrate up --directory ./_examples --migration_table_name DataMigrations
```

The table name must start with a letter and contain only letters, numbers and underscores.

This is useful when you want to manage multiple migration systems in one database (e.g., schema migrations and data migrations separately). Note that the same `--migration_table_name` value must be given to `migrate up`, `migrate version`, `migrate set` and `truncate`, otherwise they operate on the default `SchemaMigrations` table.

`truncate` keeps the migration table so that the database keeps its migration version. If you use a custom table name, pass it to `truncate` as well, otherwise the migration version is deleted:

```sh
$ wrench truncate --migration_table_name DataMigrations
```

### Apply single DDL/DML

```sh
$ wrench apply --ddl ./_examples/ddl.sql
```

This applies single DDL or DML.

### Wait for long-running DDL operations

```sh
$ wrench migrate up --directory ./_examples --wait-long-running-operation
```

By default, the whole command must finish within `--timeout` (1 hour by default).
DDL such as adding an index on a large table can take longer than that, and when the timeout is exceeded while waiting,
the DDL keeps running on Cloud Spanner but `SchemaMigrations` is left dirty.

With `--wait-long-running-operation`, `--timeout` still applies to submitting each request, but wrench polls the long-running
operation until it completes without a deadline, and then records the migration version.
The operation name is printed so that it can be inspected with `gcloud spanner operations describe` as well.

Ctrl-C (or SIGTERM) stops waiting, but it does not cancel the operation: the DDL keeps running on Cloud Spanner,
and for `migrate up` the version is left dirty. In that case wrench prints the `gcloud spanner operations describe` and
`gcloud spanner operations cancel` commands for the operation. After the operation finishes, check the schema and fix
the version with `wrench migrate set`.
This flag is also effective for `wrench apply --ddl`.

Use `wrench [command] --help` for more information about a command.

### Embed migrations file to 1 binary

`github.com/cloudspannerecosystem/wrench/cmd.CustomFileSystemFunc` is used to embed migration files into one binary.
By default, `os.DirFS(".")` is used to read files.
If you want to use a custom file system, you can create a custom wrench binary with overwrite this value.
For example, you can use `embed.FS` to read files to create custom binary like below.

```go
//go:embed schema.sql migrations/*.sql
var SpannerSchemaFS embed.FS

func init() {
    cmd.CustomFileSystemFunc = func() fs.FS {
        return SpannerSchemaFS
    }
}
```

## Contributions

Please read the [contribution guidelines](CONTRIBUTING.md) before submitting
pull requests.

### How to run tests locally

Run the test suite with:

```
$ make test
```

When `SPANNER_EMULATOR_HOST` is unset, the tests reuse an emulator at
`localhost:9010` if one is running. Otherwise, they start the Spanner emulator
with Docker, or Podman if Docker cannot start it. The container uses dynamically
assigned host ports, and the test harness creates the test instance and stops
the container when the test process exits.

When `SPANNER_EMULATOR_HOST` is set, the tests use that address. They fail if no
emulator is reachable there and do not start a container. A locally running
emulator on `localhost:9010` also gets its test instance created automatically.

The `make test` target disables Go's test result cache so the emulator setup and
tests run on every invocation. Docker or Podman is required only when the tests
need to start a local emulator.

To use an emulator at another address, set `SPANNER_EMULATOR_HOST` and initialize
its test instance before running `make test`.

## License

Copyright 2019 Mercari, Inc.

Licensed under the MIT License.
