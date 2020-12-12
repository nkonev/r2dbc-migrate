FROM adoptopenjdk:11.0.9.1_1-jre-hotspot-focal

WORKDIR /

COPY ./r2dbc-migrate.jar /r2dbc-migrate.jar
ENTRYPOINT ["java", "-jar", "/r2dbc-migrate.jar"]

