FROM bellsoft/liberica-openjdk-debian:17.0.6

WORKDIR /

COPY ./r2dbc-migrate.jar /r2dbc-migrate.jar
ENTRYPOINT ["java", "-jar", "/r2dbc-migrate.jar"]

