# R2DBC migration tool
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/name.nkonev.r2dbc-migrate/r2dbc-migrate-spring-boot-starter/badge.svg)](https://search.maven.org/search?q=g:name.nkonev.r2dbc-migrate%20AND%20a:r2dbc-migrate-spring-boot-starter)
[![Docker Image](https://images.microbadger.com/badges/version/nkonev/r2dbc-migrate.svg)](https://hub.docker.com/r/nkonev/r2dbc-migrate/tags)
[![Build Status](https://travis-ci.com/nkonev/r2dbc-migrate.svg?branch=master)](https://travis-ci.com/nkonev/r2dbc-migrate)

Inspired by [this](https://spring.io/blog/2020/03/12/spring-boot-2-3-0-m3-available-now) announcement.

R2DBC [page](https://r2dbc.io/).

## Supported databases
* PostgreSQL
* Microsoft SQL Server
* MySQL

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
  <artifactId>r2dbc-migrate-spring-boot-starter</artifactId>
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

As it includes shaded `spring-core`, it has weak dependency on
```
<dependency>
    <groupId>commons-logging</groupId>
    <artifactId>commons-logging</artifactId>
    <version>1.2</version>
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

