FROM adoptopenjdk:11.0.6_10-jre-hotspot-bionic

RUN mkdir -p /migrations/postgresql && mkdir -p /migrations/mssql

WORKDIR /

COPY ./target/r2dbc-migrate-*.jar /r2dbc-migrate.jar
ENTRYPOINT ["java", "-jar", "/r2dbc-migrate.jar"]

