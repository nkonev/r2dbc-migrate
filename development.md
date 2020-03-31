```
docker-compose down -v; docker-compose up -d; docker-compose logs -f postgresql
```

# Building
```bash
docker build . --tag nkonev/r2dbc-migrate:latest --tag nkonev/r2dbc-migrate:0.0.1

docker push nkonev/r2dbc-migrate:0.0.1
docker push nkonev/r2dbc-migrate:latest
```
