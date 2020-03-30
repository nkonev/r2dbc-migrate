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

```
docker-compose down -v; docker-compose up -d; docker-compose logs -f postgresql
```