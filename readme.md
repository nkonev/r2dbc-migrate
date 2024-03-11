# R2DBC migration library
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/name.nkonev.r2dbc-migrate/r2dbc-migrate-spring-boot-starter/badge.svg)](https://central.sonatype.com/namespace/name.nkonev.r2dbc-migrate)
[![Build Status](https://github.com/nkonev/r2dbc-migrate/workflows/Java%20CI%20with%20Maven/badge.svg)](https://github.com/nkonev/r2dbc-migrate/actions)

Inspired by [this](https://spring.io/blog/2020/03/12/spring-boot-2-3-0-m3-available-now) announcement. R2DBC [page](https://r2dbc.io/).

## Supported databases
* PostgreSQL
* Microsoft SQL Server
* H2
* MariaDB
* MySQL


It supports user-provided dialect. You can pass implementation of `SqlQueries` interface to the `migrate()` method. If you use Spring Boot, just define a bean of type `SqlQueries`. Example [SimplePostgresqlDialect](https://github.com/nkonev/r2dbc-migrate/blob/73acee24ee2963a0ffbc2f6ae64d07144367ce2f/r2dbc-migrate-core/src/test/java/name/nkonev/r2dbc/migrate/core/PostgresTestcontainersTest.java#L448).

## Features
* No need the dedicated JDBC params, r2dbc-migrate reuses your R2DBC `ConnectionFactory` provided by Spring Boot
* Convention-based file names, for example `V3__insert_to_customers__split,nontransactional.sql`
* Reasonable defaults. By convention, in case [Spring Boot](https://github.com/nkonev/r2dbc-migrate/blob/d65c7c49512a598dc4cc664bc33f78cb57ef3c43/r2dbc-migrate-spring-boot-starter/src/main/java/name/nkonev/r2dbc/migrate/autoconfigure/R2dbcMigrateAutoConfiguration.java#L60) SQL files should be placed in `classpath:/db/migration/*.sql`. It is configurable through `r2dbc.migrate.resources-paths`.
* Pre-migration scripts, for example `V0__create_schemas__premigration.sql`. Those scripts are invoked every time before entire migration process(e. g. before migration tables created), so you need to make them idempotent. You can use zero or negative version number(s): `V-1__create_schemas__nontransactional,premigration.sql`. See [example](https://github.com/nkonev/r2dbc-migrate/tree/master/r2dbc-migrate-core/src/test/resources/migrations/postgresql_premigration).
* It waits until a database has been started, then performs test query, and validates its result. This can be useful for the initial data loading into database with docker-compose
* Large SQL files support: migrations files larger than `-Xmx`: file will be split line-by-line (`split` modifier), then it will be loaded by chunks into the database. [Example](https://github.com/nkonev/r2dbc-migrate/blob/2.10.0/r2dbc-migrate-core/src/test/java/name/nkonev/r2dbc/migrate/core/PostgresTestcontainersTest.java#L230).
* It supports lock, that make you able to start number of replicas your microservice, without care of migrations collide each other. Database-specific lock tracking [issue](https://github.com/nkonev/r2dbc-migrate/issues/28).
* Each migration runs in the separated transaction by default
* It also supports `nontransactional` migrations, due to SQL Server 2017 prohibits `CREATE DATABASE` in the transaction
* First-class Spring Boot integration, see example below
* Also, you can use this library without Spring (Boot), see library example below
* This library tends to be non-invasive, consequently it intentionally doesn't try to parse SQL and make some decisions relying on. So (in theory) you can freely update the database and driver's version
* Two modes of picking migration files 
    * index.html-like with explicit paths as in Liquibase - type `JUST_FILE`
    * directories-scanning like as in Flyway - type `CONVENTIONALLY_NAMED_FILES`

See the example in the `two-modes` branch https://github.com/nkonev/r2dbc-migrate-example/tree/two-modes.

All available configuration options are in [R2dbcMigrateProperties](https://github.com/nkonev/r2dbc-migrate/blob/master/r2dbc-migrate-core/src/main/java/name/nkonev/r2dbc/migrate/core/R2dbcMigrateProperties.java) class.
Their descriptions are available in your IDE Ctrl+Space help or in [spring-configuration-metadata.json](https://github.com/nkonev/r2dbc-migrate/blob/master/r2dbc-migrate-spring-boot-starter/src/main/resources/META-INF/spring-configuration-metadata.json) file.

## Limitations
* Currently, this library heavy relies on the upsert-like syntax like `CREATE TABLE ... ON CONFLICT DO NOTHING`.
Because this syntax isn't supported in H2 in PostresSQL compatibility mode, as a result, this library [can't be](https://github.com/nkonev/r2dbc-migrate/issues/21) run against H2 with `MODE=PostgreSQL`. Use [testcontainers](https://github.com/nkonev/r2dbc-migrate-example) with the real PostgreSQL.
* Only forward migrations are supported. No backward migrations.
* No [checksum](https://github.com/nkonev/r2dbc-migrate/issues/5) validation. As a result [repeatable](https://github.com/nkonev/r2dbc-migrate/issues/9) migrations aren't supported.

## Compatibility (r2dbc-migrate, R2DBC Spec, Java, Spring Boot ...)
See [here](https://github.com/nkonev/r2dbc-migrate/issues/27#issuecomment-1404878933)

## Download

### Spring Boot Starter
```xml
<dependency>
  <groupId>name.nkonev.r2dbc-migrate</groupId>
  <artifactId>r2dbc-migrate-spring-boot-starter</artifactId>
  <version>VERSION</version>
</dependency>
```

### Only library
```xml
<dependency>
    <groupId>name.nkonev.r2dbc-migrate</groupId>
    <artifactId>r2dbc-migrate-core</artifactId>
    <version>VERSION</version>
</dependency>
```

If you use library, you need also use some implementation of `r2dbc-migrate-resource-reader-api`, for example:
```xml
<dependency>
    <groupId>name.nkonev.r2dbc-migrate</groupId>
    <artifactId>r2dbc-migrate-resource-reader-reflections</artifactId>
    <version>VERSION</version>
</dependency>
```
See `Library example` below.


## Spring Boot example
https://github.com/nkonev/r2dbc-migrate-example

## Spring Native example
See example [here](https://github.com/nkonev/r2dbc-migrate-example/tree/native).

## Library example
https://github.com/nkonev/r2dbc-migrate-example/tree/library
