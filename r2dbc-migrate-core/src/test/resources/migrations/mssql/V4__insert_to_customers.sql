USE my_db; -- use other db to switch default charset (=collation in SQL Server)
insert into customer(first_name, last_name) values ('Muhammad', 'Ali'), ('Name', 'Surname');
insert into customer(first_name, last_name) values ('Покупатель', 'Фамилия');
USE master;