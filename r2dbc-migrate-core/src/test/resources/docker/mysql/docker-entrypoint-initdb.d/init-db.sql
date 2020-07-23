CREATE DATABASE r2dbc;
use r2dbc;
GRANT ALL PRIVILEGES ON r2dbc.* TO 'mysql-user'@'%' IDENTIFIED BY 'mysql-password';

create database `my scheme`;
use `my scheme`;
GRANT ALL PRIVILEGES ON `my scheme`.* TO 'mysql-user'@'%' IDENTIFIED BY 'mysql-password';
