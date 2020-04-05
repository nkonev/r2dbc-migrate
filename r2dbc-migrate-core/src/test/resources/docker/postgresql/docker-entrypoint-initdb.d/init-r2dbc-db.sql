-- ALTER SYSTEM SET max_connections = 400;
-- Uncomment if you need to view the full postgres logs (SQL statements, ...) via `docker logs -f postgresql-test`
-- ALTER SYSTEM SET log_statement = 'all';
ALTER SYSTEM SET synchronous_commit = 'off'; -- https://postgrespro.ru/docs/postgrespro/9.5/runtime-config-wal.html#GUC-SYNCHRONOUS-COMMIT
ALTER SYSTEM SET shared_buffers='512MB';
ALTER SYSTEM SET fsync=FALSE;
ALTER SYSTEM SET full_page_writes=FALSE;
ALTER SYSTEM SET commit_delay=100000;
ALTER SYSTEM SET commit_siblings=10;
ALTER SYSTEM SET work_mem='256MB';

create user r2dbc with password 'r2dbcPazZw0rd';
create database r2dbc with owner r2dbc;

\connect r2dbc;


-- https://www.endpoint.com/blog/2012/10/30/postgresql-autoexplain-module
-- ALTER SYSTEM set client_min_messages = notice;
-- ALTER SYSTEM set log_min_messages = notice;
-- ALTER SYSTEM set log_min_duration_statement = -1;
-- ALTER SYSTEM set log_connections = on;
-- ALTER SYSTEM set log_disconnections = on;
-- ALTER SYSTEM set log_duration = on;

--

/*LOAD 'auto_explain';
ALTER SYSTEM set shared_preload_libraries = 'auto_explain';
ALTER SYSTEM SET auto_explain.log_min_duration=0;
ALTER SYSTEM set auto_explain.log_analyze=true;
ALTER SYSTEM set auto_explain.log_buffers=true;
ALTER SYSTEM set auto_explain.log_timing=true;
ALTER SYSTEM set auto_explain.log_verbose=true;
ALTER SYSTEM set auto_explain.log_nested_statements=true;
*/

\connect r2dbc r2dbc;
