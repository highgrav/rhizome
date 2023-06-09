# Rhizome
## Postgres (wire)-compatible hard multitenancy database framework

Rhizome is a proof of concept for implementing a PG wire-compatible server that enforces hard multitenant databases. 
Sqlite3 is used as the backing storage and query engines for the tenant databases, which allows for segregation of data 
behind the application server layer with reduced risk of inadvertent exposure (relative to either soft single-schema or 
single-database multi-schema approaches).

Rhizome currently only supports the "canonical" Sqlite datatypes, which map to Postgres 64-bit integers, 64-bit floats, 
varchar, or bytea. In addition, it will attempt to convert appropriate columns to Postgres date, timestamp with time zone, 
and boolean values, though this has not been extensively tested yet; bad data in Sqlite may result in unexepected results, 
such as dates or timestamps being coerced to "zero values."

Since Rhizome is essentially (to be reductionist) a file manager with a network interface, it outsources query management 
to Sqlite, and makes no attempt to translate pgsql to Sqlite's SQL dialect. In other words, Postgres-specific SQL will 
not work. Also, there is no attempt to replicate Postgres' `pg_catalog` (as the excellent `Postlite` framework does), so 
some tools may complain about being unable to query the system catalogs.

### Usage and Sample Implementation
Integrating Rhizome is fairly straightforward: you first set up your Deck logging, then:
- Run `rhizome.Init(...)` to set up Rhizome with any custom functions 
- Create a new `DBManager` with `rhizome.NewDBManager(...)` 
- For each Postgres connection, create a new backend handler with `rhizome.NewRhizomeBackend(...)` 
- Call `.run()` on the handler.

A sample implementation can be found in `cmd/rhizd`. You can test this by creating a new Sqlite db in `/tmp`, 
running `go run cmd/rhizd` and then attempting to connect to it via `psql`. For example, if you created a `/tmp/test.db` 
database, you can connect to it by `psql -h localhost -p 5432 -d test`.


### Warning
Rhizome is not a production framework, and the API will continue to change. If you are looking for robust, Sqlite-based 
database solutions, try:

- Postlite: Covers essentially the same ground as (and in fact inspired) Rhizome, with some nice usability extras. Mostly designed as a drop-in for ETL and other data pipelines.  https://github.com/benbjohnson/postlite
- Litestream: Near real-time replication and disaster recovery for Sqlite. https://litestream.io/
- LiteFS: FUSE filesystem layer that provides improved (compared to Litestream) transaction management. https://github.com/superfly/litefs/
- rqlite: Replicated (many readers, Raft-based writers) Sqlite server. https://rqlite.io/
- dqlite: C library to run replicated Sqlite; more efficient than rqlite. https://dqlite.io/


Rhizome doesn't pretend to be a production-ready database and lacks the consistency guarantees of the above solutions. 

In addition, ID sharding is not implemented at this layer, so you're likely to run out of file descriptors if you have a decent 
number of databases open; this risk can be reduced by implementing sharding at your application level to determine where requests 
go, and making your `FnGetFilenameFromID` function shard-aware (and thus return an `ErrWrongDBServer` if the shard doesn't reside on a 
given database). As I said, proof of concept, caveat aedificator.