drop table pics;
create table pics (
    id serial primary key,
    mime_type varchar,
    description text,
    image bytea
);
drop table files;
create table files (
    id serial primary key,
    name varchar,
    created timestamp default now(),
    modified timestamp,
    file bytea
);
drop table notes;
create table notes (
    id serial primary key,
    subject text,
    note text,
    created timestamp default now()
)