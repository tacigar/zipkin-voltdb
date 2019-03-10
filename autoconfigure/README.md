# zipkin-autoconfigure-storage-voltdb

## Overview

This is a Spring Boot [AutoConfiguration](http://docs.spring.io/spring-boot/docs/current/reference/html/using-boot-auto-configuration.html)
module that can be added to a [Zipkin Server](https://github.com/openzipkin/zipkin/tree/master/zipkin-server) 
deployment to send Spans to VoltDB.

## Experimental
* Note: This is currently experimental! *

## Quick start

JRE 8 is required to run Zipkin server.

Fetch the latest released
[executable jar for Zipkin server](https://search.maven.org/remote_content?g=io.zipkin.java&a=zipkin-server&v=LATEST&c=exec)
and build the module jar for VoltDB storage.
Run Zipkin server with the VoltDB Storage enabled.

For example (from the project root):

```bash
$ curl -sSL https://zipkin.io/quickstart.sh | bash -s
$ ./mvnw clean install -DskipTests
$ cp autoconfigure/target/*module.jar voltdb.jar
$ STORAGE_TYPE=voltdb \
    java \
    -Dloader.path='voltdb.jar,voltdb.jar!/lib' \
    -Dspring.profiles.active=voltdb \
    -cp zipkin.jar \
    org.springframework.boot.loader.PropertiesLauncher
```

After executing these steps, applications can send spans
http://localhost:9411/api/v2/spans (or the legacy endpoint http://localhost:9411/api/v1/spans)

The Zipkin server can be further configured as described in the
[Zipkin server documentation](https://github.com/openzipkin/zipkin/blob/master/zipkin-server/README.md).

### Configuration

Configuration can be applied either through environment variables or an external Zipkin
configuration file.  The module includes default configuration that can be used as a 
[reference](https://github.com/openzipkin/zipkin-gcp/tree/master/autoconfigure/storage-voltdb/src/main/resources/zipkin-server-voltdb.yml)
for users that prefer a file based approach.

#### Environment Variables

|Environment Variable           | Value            |
|-------------------------------|------------------|
|VOLTDB_HOST | host:port combination of the VoltDB endpoint. Default: localhost:21212 |
|VOLTDB_ENSURE_SCHEMA | Whether to automatically run ddl.sql if necessary. Default: true |

### Running

```bash
$ STORAGE_TYPE=voltdb \
    java \
    -Dloader.path='voltdb.jar,voltdb.jar!/lib' \
    -Dspring.profiles.active=voltdb \
    -cp zipkin.jar \
    org.springframework.boot.loader.PropertiesLauncher
```

### Testing

Once your storage is enabled, verify it is running:
```bash
$ curl -s localhost:9411/health|jq .zipkin.details.VoltDBStorage
{
  "status": "UP"
}
```
