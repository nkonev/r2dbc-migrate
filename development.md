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

# Open MySQL shell
```
docker-compose exec mysql mysql -umysql-user -pmysql-password -Dr2dbc
```

# Building
```bash
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
(cd ./docker; docker-compose down -v; docker-compose up -d)
./mvnw clean package -DenableStandaloneTests=true -DenableOomTests=true
./mvnw release:prepare -Dresume=false
./mvnw release:perform

git fetch
```
