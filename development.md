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

# Making a release
```bash
export JAVA_HOME=/usr/lib/jvm/java-17
./mvnw clean
./mvnw -Dresume=false -DskipTests release:prepare release:perform
git fetch
```

# FAQ:
## Q: Tests are failed with `com.github.dockerjava.api.exception.NotFoundException` on Linux Desktop (Fedora 35), but works on Github Actions
but user is in docker group
```
[nkonev@fedora ~]$ groups
nkonev wheel docker
```
`com.github.dockerjava.api.exception.NotFoundException: Status 404: {"message":"No such container: e24bc989de97049777f05be9f3a7cebf6389ddc349a91ea13f413a599ac4a8e1"}`
Also this is mentioned [here](https://github.com/testcontainers/testcontainers-java/issues/572#issuecomment-411703450).
A: 
```
su -
setenforce 0
```
