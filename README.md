# RHIZOME
## Postgres (wire)-compatible hard multitenancy database framework

Rhizome is a proof of concept for implementing a PG wire-compatible server that enforces hard multitenant databases. 
Sqlite3 is used as the backing storage and query engines for the tenant databases, which allows for segregation of data 
behind the application server layer with reduced risk of inadvertent exposure (relative to either soft single-schema or 
single-database multi-schema approaches).

Rhizome is not a production framework, and the API will continue to change. If you are looking for robust, Sqlite-based 
database solutions, try:

- Litestream: Near real-time replication and disaster recovery for Sqlite. https://litestream.io/
- LiteFS: FUSE filesystem layer that provides improved (compared to Litestream) transaction management. https://github.com/superfly/litefs/
- rqlite: Replicated (many readers, Raft-based writers) Sqlite server. https://rqlite.io/
- dqlite: C library to run replicated Sqlite; more efficient than rqlite. https://dqlite.io/

Rhizome doesn't pretend to be production-ready database and lacks the consistency guarantees of the above solutions. In 
addition, ID sharding is not implemented at this layer, so you're likely to run out of file descriptors if you have a decent 
number of databases open; this risk can be reduced by implementing sharding at your application level to determine where requests 
go, and making your `FnGetFilenameFromID` function shard-aware (and thus return an `ErrWrongDBServer` if the shard doesn't reside on a 
given database). As I said, proof of concept, caveat aedificator.