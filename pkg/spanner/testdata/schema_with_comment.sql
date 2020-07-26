CREATE TABLE SchemaMigrations (
  Version INT64 NOT NULL,
  Dirty BOOL NOT NULL,
) PRIMARY KEY(Version);

CREATE TABLE Singers (
  SingerID STRING(36) NOT NULL,
  FirstName STRING(1024),
) PRIMARY KEY(SingerID);

# this is an inline comment
CREATE TABLE TableWithComments (
  /* this is a multiline comment
  on two lines */
  ID STRING(36) NOT NULL, -- this is an inline comment
  /* this is an inline comment */

  /* column commented out
  Name STRING(36) NOT NULL
   */
) PRIMARY KEY(ID); # another comment
