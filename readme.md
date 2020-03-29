# TODO
* waitFor db start
  * grace period
* split by newline
* lock like in my mybatis

https://github.com/microsoft/mssql-docker/issues/355#issuecomment-530063302
```
/opt/mssql/bin/mssql-conf traceflag 3979 on
/opt/mssql/bin/mssql-conf set control.alternatewritethrough 0
/opt/mssql/bin/mssql-conf set control.writethrough 0
```

Deal with mssql
```bash
docker-compose exec mssql /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'yourStrong(!)Password'
```
