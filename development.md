# Reset & open PostgreSQL logs
```
docker-compose down -v; docker-compose up -d; docker-compose logs -f postgresql
```

# Open MS SQL shell
```bash
docker-compose exec mssql /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'yourStrong(!)Password'
```

# Open PostgreSQL shell
```
docker-compose exec postgresql psql -U r2dbc
```

# Building
```bash
./mvnw clean package && docker build . --tag nkonev/r2dbc-migrate:latest --tag nkonev/r2dbc-migrate:0.0.4
git tag 0.0.4
git push origin HEAD --tags
docker push nkonev/r2dbc-migrate:0.0.4
docker push nkonev/r2dbc-migrate:latest
```
