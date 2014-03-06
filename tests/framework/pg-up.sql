CREATE TABLE IF NOT EXISTS "usersTable" (
    "ID" serial primary key,
    "Name" varchar(128) NOT NULL DEFAULT 'someValue',
    "IntegerField" integer DEFAULT 0,
    "DateField" date DEFAULT NULL,
    "TimeField" time DEFAULT NULL,
    "DateTimeField" timestamp DEFAULT NULL,
    "isSystem" boolean DEFAULT TRUE,
    "xCoord" float(32) DEFAULT NULL,
    "AccountID" integer DEFAULT NULL,
    "CustomPropertyValue" integer DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS "accountsTable" (
    "AccountID" serial primary key,
    "EmailField" varchar(32),
    "PhoneField" varchar(32)
);

CREATE TABLE IF NOT EXISTS "tagsTable" (
    "TagID" serial primary key,
    "TagName" varchar(32),
    "TagWeight" integer
);

CREATE TABLE IF NOT EXISTS "statusesTable" (
    "ID" serial primary key,
    "StatusName" varchar(32),
    "StatusWeight" integer,
    "userID" integer
);

CREATE TABLE IF NOT EXISTS "profilesTable" (
    "ID" serial primary key,
    "Avatar" varchar(32),
    "LikesCount" integer,
    "userID" integer
);

CREATE TABLE IF NOT EXISTS "passportsTable" (
    "ID" serial primary key,
    "Series" integer,
    "Number" integer,
    "userID" integer
);

CREATE TABLE IF NOT EXISTS "documentsTable" (
    "ID" serial primary key,
    "Series" integer,
    "Number" integer,
    "userID" integer
);

CREATE TABLE IF NOT EXISTS "users_tags_relations" (
    "ID" serial primary key,
    "userID" integer,
    "tagID" integer
);

CREATE TABLE IF NOT EXISTS "tableWithoutPrimaryKey" (
    "Name" varchar(32),
    "Value" integer,
    "Time" timestamp,
    "userID" integer
);

CREATE TABLE IF NOT EXISTS "multiMappedTable" (
    "ID" serial primary key,
    "Name" varchar(32),
    "authorID" integer,
    "userID" integer
);