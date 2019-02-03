# zipkin-voltdb
Shared libraries that provide Zipkin integration with VoltDB. Requires
JRE 8.

*This is not production ready at the moment. Things will change!*

## Quick Start
Make sure you have [VoltDB](https://www.voltdb.com/try-voltdb/open-source-edition/) and it is running.
Hit http://localhost:8080 to test that VoltDB's console is available.

Then, package and start Zipkin with VoltDB support.

```bash
$ curl -sSL https://zipkin.io/quickstart.sh | bash -s
$ curl -sSL https://github.com/adriancole/zipkin-voltdb/releases/download/latest/voltdb.jar > voltdb.jar
$ STORAGE_TYPE=voltdb \
    java \
    -Dloader.path='voltdb.jar,voltdb.jar!/lib' \
    -Dspring.profiles.active=voltdb \
    -cp zipkin.jar \
    org.springframework.boot.loader.PropertiesLauncher
```

After executing these steps, applications can send spans to
http://localhost:9411/api/v2/spans (or the legacy endpoint http://localhost:9411/api/v1/spans)

If you have any traces, they will show up at http://localhost:9411/zipkin

## Storage
The library that persists and queries collected spans is called
`StorageComponent`. The [storage](storage) module supports the Zipkin Api and all
collector components.

## Autoconfigure
The component in a zipkin server that configures settings for storage is
is called auto-configuration, a Spring Boot concept. The [autoconfigure](storage)
module plugs into an existing Zipkin server adding VoltDB support.
