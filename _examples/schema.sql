CREATE TABLE Singers (
  SingerID STRING(36) NOT NULL,
  FirstName STRING(1024),
  LastName STRING(1024),
) PRIMARY KEY(SingerID);

CREATE TABLE SchemaMigrations (
  Version INT64 NOT NULL,
  Dirty BOOL NOT NULL,
) PRIMARY KEY(Version);

CREATE TABLE SchemaMigrationsHistory (
  Version INT64 NOT NULL,
  Dirty BOOL NOT NULL,
  Created TIMESTAMP NOT NULL OPTIONS (
    allow_commit_timestamp = true
  ),
  Modified TIMESTAMP NOT NULL OPTIONS (
    allow_commit_timestamp = true
  ),
) PRIMARY KEY(Version);
