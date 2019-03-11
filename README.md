# zipkin-voltdb
Shared libraries that provide Zipkin integration with VoltDB. Requires JRE 8.

*This is not production ready at the moment. Things will change!*

## Why?
We've had many requests for late sampling, a SQL replacement for MySQL and something that supports higher
ingest loads in general. [Bas van Beek](https://github.com/basvanbeek) used VoltDB for triaging timing
data of another kind (marathons) and had the idea the storage could be ideal for Zipkin. [Lance](https://github.com/llinder),
[Zoltan](https://github.com/abesto), Bas and I spiked some effort towards this during our [Pow-wow](https://cwiki.apache.org/confluence/display/ZIPKIN/2019-01-30+Zipkin+PPMC+Pow-wow). 

## What's different?

### Near Real Time Dependency Linking (Implemented)
This experiment includes advanced features not present in other tools, such as automatic trace analysis.
Notably, we keep state about last updates to traces, and then run some analysis to see if they are "done"
or not. If so, we immediately perform dependency link aggregation as opposed to waiting for a cron to go
off. In "laptop tests", we've found backlog performing as expected and it is really easy to tell using the
VoltDB Console.

![voltdb](https://user-images.githubusercontent.com/64215/52180732-60b7ce80-27ea-11e9-8fa4-568700125d1a.gif)

### Streaming export (Partially implemented)
We can [use VoltDB export tables](https://github.com/adriancole/zipkin-voltdb/pull/3) to
stream data to other systems regardless of whether it is individual spans, metrics derived from them
or complete traces. This would support use cases like shipping data to sinks that need 100% data like [Haystack Trends](https://github.com/ExpediaDotCom/haystack-trends).

### Downsampling (Partially implemented)
A lot of storage problems are overload in nature. VoltDB is in-memory, but includes TTL of both time and also
row count. Our schema includes an "export table" which contains trace IDs that should be sent downstream for
long term storage. The first implementation of export sampling is probabilistic though more sophisticated
mechanisms can be made.

Regardless of export or not, service graph dependency linking happens when a trace is considered "done".

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
