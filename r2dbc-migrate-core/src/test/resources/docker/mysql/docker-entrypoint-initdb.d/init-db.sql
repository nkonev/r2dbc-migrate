CREATE DATABASE r2dbc;
use r2dbc;

GRANT ALL PRIVILEGES ON r2dbc.* TO 'mysql-user'@'%' IDENTIFIED BY 'mysql-password';