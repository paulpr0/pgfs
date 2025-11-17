# pgfs

*pgfs* exposes data inside a PostgreSQL database as files in a fuse filesystem
To run as a binary application, provide a config file in toml format
(see the docs or examples) to specify the database you want to connect to,
and what tables/queries to expose and how.

*pgfs* aims to be fully functional, and allows you to (optionally) read, create, modify, rename and
delete files which map to data stored in a PostgreSQL database. Default behaviour is to
mirror the filesystem operation in the database (so a read is a select, and a delete deletes etc.),
but you can fully configure the SQL queries used for each operation so that, for example, a delete
might just mark a status field in a record as archived, or a write might be set to create a new
version of a file, leaving the old one in tact (beware some programs do a lot of small writes).

This is a FUSE filesystem, and particular attention has not been paid to raw performance.
In particular, frequent or large writes to bytea or text fields will perform poorly as Postgres is not designed
to write deltas to individual fields. It is not possible to store files larger than the database limit
for a bytea field (1 or 2 GB at present depending on version) and you would not want to do so
as the performance for writes would be awful. Fuse filesystems tend to write data in 4k blocks which
would kill write performance for any typical non-text files (writing a ~10mb file would incur ~2.5k writes)
so by default we cache sequential writes up to a configurable size. See ***caching*** for more details or
to modify or turn off this feature. A typical slowdown vs local file writing might be 1-2 orders of magnitude.
A write of a 10mb file on my laptop with the default cache size of 2mb takes 60 times longer (2 seconds)
than a straight cp on the same filesystem.

Whilst Postgres can store larger files using blobs
there is no explicit blob support, but it might work if you provide the queries in config. If you
try it, please let me know how you get on.

This is currently experimental software and as such it is not recommended that you use it
for production systems, or enable write access to data you are not prepared to see corrupted.
Warnings aside, if you configure read only mode, and set up a Postgres user with only read permissions
your data should be safe, and this implementation is single threaded and blocking, so should not
exhaust your resources easily.

 Bug reports, bug fixes, pull requests, examples, feature requests and any other feedback is welcome.
 If you try this out and have 5 minutes to drop me a quick message to tell me what you think that would
 be great.
