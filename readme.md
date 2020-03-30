# R2DBC migration tool and library

## Supported databases
* PostgreSQL
* MS SQL

## Features
* docker friendly - it waits for until database started
* split large file by newline by chunks
* support concurrent migrations by locking

## Todo
* make non-transactional migrations for creating databases and schemas in MS SQL
* introduce library
* more tests

## Open MS SQL shell
```bash
docker-compose exec mssql /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'yourStrong(!)Password'
```

## Open PostgreSQL shell
```
docker-compose exec postgresql psql -U r2dbc
```

Create database and schema in it in MS SQL 
```java
.log("Make migration work")
.flatMapMany(
        connection -> {
            return Mono.from(connection.createStatement("create database db6").execute())
                    .then(Mono.from(connection.createStatement("insert into migrations(id, description) values(6, 'six')").execute()))
                    .then(Mono.from(connection.createStatement("exec('use db6; exec sp_executesql N''create schema p6'' '); use master;").execute()))
                    .then(Mono.from(connection.createStatement("insert into migrations(id, description) values(7, 'seven')").execute()))
                    .then();
        }
);
```