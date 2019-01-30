# zipkin-voltdb
Shared libraries that provide Zipkin integration with VoltDB. Requires
JRE 8.

## Storage
The library that persists and queries collected spans is called
`StorageComponent`. The [storage] module supports the Zipkin Api and all
collector components.

## Autoconfigure
The component in a zipkin server that configures settings for storage is
is called auto-configuration, a Spring Boot concept. The [autoconfigure]
module plugs into an existing Zipkin server adding VoltDB support.
