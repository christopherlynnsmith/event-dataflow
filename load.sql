create database test;

drop table movies;

create table movies (
    id varchar(100) unique not null,
    moviename varchar(200),
    rating int
);


insert ignore into test.movies values ( '1','hello world1',5);

insert ignore into test.movies values ( '2','hello world2',4);

insert ignore into test.movies values ( '3','hello world3',3);