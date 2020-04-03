# R2DBC migration tool

Inspired by [this](https://spring.io/blog/2020/03/12/spring-boot-2-3-0-m3-available-now) announcement.

R2DBC [page](https://r2dbc.io/).

## Supported databases
* PostgreSQL
* Microsoft SQL Server

## Features
* Filename-based `V3__insert_to_customers__split,nontransactional.sql`: parts separated by two underscores, where last part is flags - one of `split`, `nontransactional`
* Waiting for until database started
* Validation query and (optionally) checking its result
* Split large file to chunks by newline. You can migrate file larger than your `-Xmx`
* Support concurrent migrations in microservices by locking mechanism

## Download

### Docker
```
docker pull nkonev/r2dbc-migrate:latest
```

### Spring Boot Starter
```
<dependency>
  <groupId>name.nkonev.r2dbc-migrate</groupId>
  <artifactId>spring-boot-starter-r2dbc-migrate</artifactId>
  <version>VERSION</version>
</dependency>
```

### Only library
```
<dependency>
  <groupId>name.nkonev.r2dbc-migrate</groupId>
  <artifactId>r2dbc-migrate-core</artifactId>
  <version>VERSION</version>
</dependency>
```

### Standalone application
```
<dependency>
  <groupId>name.nkonev.r2dbc-migrate</groupId>
  <artifactId>r2dbc-migrate-standalone</artifactId>
  <version>VERSION</version>
</dependency>
```

## Example
https://github.com/nkonev/r2dbc-migrate-example

