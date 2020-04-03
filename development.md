# Reset & open PostgreSQL logs
```
cd ./docker
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
(cd ./docker; docker-compose down -v; docker-compose up -d)
./mvnw clean package
./mvnw release:prepare -Dresume=false
./mvnw release:perform

(cd ./r2dbc-migrate-standalone; rm ./target/*-javadoc.jar ./target/*-sources.jar; docker build . --tag nkonev/r2dbc-migrate:latest --tag nkonev/r2dbc-migrate:0.0.13)
docker push nkonev/r2dbc-migrate:0.0.13
docker push nkonev/r2dbc-migrate:latest

git push origin HEAD --tags
```
