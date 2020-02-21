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

Get binary from [release page](https://github.com/mercari/wrench/releases).
Or, you can use Docker container: [mercari/wrench](https://hub.docker.com/r/mercari/wrench).

## Usage

### Prerequisite

```sh
export SPANNER_PROJECT_ID=your-project-id
export SPANNER_INSTANCE_ID=your-instance-id
export SPANNER_DATABASE_ID=your-database-id
```

You can also specify project id, instance id and database id by passing them as command arguments.

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

### Apply single DDL/DML

```sh
$ wrench apply --ddl ./_examples/ddl.sql
```

This applies single DDL or DML.

Use `wrench [command] --help` for more information about a command.


## Contributions

Please read the [contribution guidelines](CONTRIBUTING.MD) before submitting
pull requests.

## License

Copyright 2019 Mercari, Inc.

Licensed under the MIT License.
