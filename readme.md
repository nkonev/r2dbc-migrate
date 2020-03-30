# Features
* wait for db starts
* split large file by newline by chunks
* support concurrent migrations by locking

# Todo
* more tests

# Open MS SQL shell
```bash
docker-compose exec mssql /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'yourStrong(!)Password'
```

# Open PostgreSQL shell
```
docker-compose exec postgresql psql -U r2dbc
```