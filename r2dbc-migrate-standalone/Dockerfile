FROM bellsoft/liberica-openjdk-debian:11.0.12

WORKDIR /

COPY ./r2dbc-migrate.jar /r2dbc-migrate.jar
ENTRYPOINT ["java", "-jar", "/r2dbc-migrate.jar"]

