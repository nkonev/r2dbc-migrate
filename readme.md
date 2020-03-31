# R2DBC migration tool

## Supported databases
* PostgreSQL
* Microsoft SQL Server

## Features
* Filename-based `V3__insert_to_customers__split,nontransactional.sql`: parts separated by two underscores, where last part is flags - one of `split`, `nontransactional`
* docker friendly - it waits for until database started
* split large file by newline by chunks
* support concurrent migrations in microservices by locking

## Download
```
docker pull nkonev/r2dbc-migrate:latest
```

## Todo
* introduce library
* more tests
