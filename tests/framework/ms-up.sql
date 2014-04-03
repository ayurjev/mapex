IF OBJECT_ID('usersTable', 'Table') IS NULL
CREATE TABLE "usersTable" (
	"ID" BIGINT PRIMARY KEY IDENTITY,
	"Name" NVARCHAR(50) NOT NULL,
	"IntegerField" BIGINT NOT NULL DEFAULT 0,
  "DateField" DATE,
	"TimeField" TIME,
	"DateTimeField" DATETIME2,
	"isSystem" BIT DEFAULT 1,
  "xCoord" FLOAT,
  "AccountID" BIGINT,
  "CustomPropertyValue" BIGINT
);

IF OBJECT_ID('accountsTable', 'Table') IS NULL
CREATE TABLE "accountsTable" (
  "AccountID" BIGINT PRIMARY KEY IDENTITY,
  "EmailField" NVARCHAR(32),
  "PhoneField" NVARCHAR(32)
);

IF OBJECT_ID('tagsTable', 'Table') IS NULL
CREATE TABLE "tagsTable" (
  "TagID" BIGINT PRIMARY KEY IDENTITY,
  "TagName" NVARCHAR(32),
  "TagWeight" BIGINT
);

IF OBJECT_ID('statusesTable', 'Table') IS NULL
CREATE TABLE "statusesTable" (
  "ID" BIGINT PRIMARY KEY IDENTITY,
  "StatusName" NVARCHAR(32),
  "StatusWeight" BIGINT,
  "userID" BIGINT
);

IF OBJECT_ID('profilesTable', 'Table') IS NULL
CREATE TABLE "profilesTable" (
  "ID" BIGINT PRIMARY KEY IDENTITY,
  "Avatar" NVARCHAR(32),
  "LikesCount" BIGINT,
  "userID" BIGINT
);

IF OBJECT_ID('passportsTable', 'Table') IS NULL
CREATE TABLE "passportsTable" (
  "ID" BIGINT PRIMARY KEY IDENTITY,
  "Series" BIGINT,
  "Number" BIGINT,
  "userID" BIGINT
);

IF OBJECT_ID('documentsTable', 'Table') IS NULL
CREATE TABLE "documentsTable" (
  "ID" BIGINT PRIMARY KEY IDENTITY,
  "Series" BIGINT,
  "Number" BIGINT,
  "userID" BIGINT
);

IF OBJECT_ID('documentsWithoutAutoincrementTable', 'Table') IS NULL
CREATE TABLE "documentsWithoutAutoincrementTable" (
  "Series" BIGINT PRIMARY KEY,
  "Number" BIGINT,
  "userID" BIGINT
);

IF OBJECT_ID('users_tags_relations', 'Table') IS NULL
CREATE TABLE "users_tags_relations" (
  "ID" BIGINT PRIMARY KEY IDENTITY,
  "userID" BIGINT,
  "tagID" BIGINT
);

IF OBJECT_ID('tableWithoutPrimaryKey', 'Table') IS NULL
CREATE TABLE "tableWithoutPrimaryKey" (
  "Name" NVARCHAR(32),
  "Value" BIGINT,
  "Time" DATETIME,
  "userID" BIGINT
);

IF OBJECT_ID('multiMappedTable', 'Table') IS NULL
CREATE TABLE "multiMappedTable" (
  "ID" BIGINT PRIMARY KEY IDENTITY,
  "Name" NVARCHAR(32),
  "authorID" BIGINT,
  "userID" BIGINT
);

IF OBJECT_ID('housesTable', 'Table') IS NULL
CREATE TABLE "housesTable" (
  "userID" BIGINT PRIMARY KEY IDENTITY,
  "address" NVARCHAR(32)
);