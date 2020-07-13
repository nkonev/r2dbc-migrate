# R2DBC migration tool
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/name.nkonev.r2dbc-migrate/r2dbc-migrate-spring-boot-starter/badge.svg)](https://search.maven.org/search?q=g:name.nkonev.r2dbc-migrate%20AND%20a:r2dbc-migrate-spring-boot-starter)
[![Docker Image Version (latest semver)](https://img.shields.io/docker/v/nkonev/r2dbc-migrate)](https://hub.docker.com/r/nkonev/r2dbc-migrate/tags)
[![Build Status](https://travis-ci.com/nkonev/r2dbc-migrate.svg?branch=master)](https://travis-ci.com/nkonev/r2dbc-migrate)

Inspired by [this](https://spring.io/blog/2020/03/12/spring-boot-2-3-0-m3-available-now) announcement.

R2DBC [page](https://r2dbc.io/).

## Supported databases
* PostgreSQL
* Microsoft SQL Server
* MySQL
* H2

## Features
* Convention-based file names, for example `V3__insert_to_customers__split,nontransactional.sql`
* It waits until database have been started, there is test query, and validation result of. This can be useful to initial load data into database with docker-compose
* Supports migrations files larger than `-Xmx`: file will be splitted line-by-line (`split` modifier), then it will be loaded by chunks into database
* Lock support, that make you able to start number of replicas your microservice, without care of migrations will collide each other
* Each migration runs in the separated transaction
* It also supports `nontransactional` migrations, due to SQL Server prohibits `CREATE DATABASE` in the transaction
* Docker image

All available configuration options are in [R2dbcMigrateProperties](https://github.com/nkonev/r2dbc-migrate/blob/master/r2dbc-migrate-core/src/main/java/name/nkonev/r2dbc/migrate/core/R2dbcMigrateProperties.java) class.
Their descriptions are available in your IDE Ctrl+Space help or in [spring-configuration-metadata.json](https://github.com/nkonev/r2dbc-migrate/blob/master/r2dbc-migrate-spring-boot-starter/src/main/resources/META-INF/spring-configuration-metadata.json) file.

## Download

### Docker
```
docker pull nkonev/r2dbc-migrate:latest
```

### Spring Boot Starter
```xml
<dependency>
  <groupId>name.nkonev.r2dbc-migrate</groupId>
  <artifactId>r2dbc-migrate-spring-boot-starter</artifactId>
  <version>VERSION</version>
</dependency>
```

### Only library
```xml
<dependency>
    <groupId>name.nkonev.r2dbc-migrate</groupId>
    <artifactId>r2dbc-migrate-core</artifactId>
    <version>VERSION</version>
</dependency>
```

It has weak dependency on `spring-core`, in order to read protocols like `file:/`, `classpath:/`, so you need to add
```xml
<dependency>
    <groupId>org.springframework</groupId>
    <artifactId>spring-core</artifactId>
    <version>VERSION</version>
</dependency>
```

### Standalone application

If you want to build your own docker image you will be able to do this
```bash
curl -Ss https://repo.maven.apache.org/maven2/name/nkonev/r2dbc-migrate/r2dbc-migrate-standalone/VERSION/r2dbc-migrate-standalone-VERSION.jar > /tmp/migrate.jar
```

## Spring Boot Example
https://github.com/nkonev/r2dbc-migrate-example

## Library example
https://github.com/nkonev/r2dbc-migrate-example/tree/library
