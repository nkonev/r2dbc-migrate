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
