This is a simple example of a Rhizome server with some basic capabilities. 

Available flags are:
- `port`: The port for the server to listen on. Defaults to `5432`.
- `dir`: The directory to store Sqlite files in. Defaults to `/tmp`.
- `ll`: The logging level (using syslog convention) to log at. Defaults to `3` (error and above).
- `tlsdir`: The directory to find TLS cert and key files, if any. If this is set, `rhizd` will attempt to enable TLS.
- `cert`: Name of the TLS certificate file; `rhizd` will look for it in `tlsdir`. This must be set if `tlsdir` is set.
- `key`: Name of the TLS key file; `rhizd` will look for it in `tlsdir`. This must be set if `tlsdir` is set.
- `udir`: The directory to file user and group files, if any. If this is set, `rhizd` will attempt to load user and group files.
- `ufile`: The name of an Apache htpasswd file to load users from; `rhizd` will look for it in `udir`. This must be set if `udir` is set.
- `gfile`: The name of an Apache group file (where a group maps to a Rhizome database name) to load group and user mappings from; `rhizd` will look for it in `udir`. This is optional, and if it is not set, then `rhizd` will only authenticate using the htpasswd file.
