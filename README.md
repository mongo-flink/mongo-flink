# MongoFlink
MongoFlink is a connector between MongoDB and Apache Flink. It acts as a Flink sink (and an experimental Flink source),
and provides transaction mode(which ensures exactly-once semantics) for MongoDB 4.2 above, and non-transaction mode
for MongoDB 3.0 above.

MongoFlink is in its early phase, and any use, feedback or contribution is welcome!

# Start to use
## Prerequisite

- Flink 1.12 above. MongoFlink is built on top of the new sink API added in FLIP-143 or Flink 1.12.0.
- MongoDB 3.0 above. The official MongoDB Java driver supports 3.0 above.
- JDK 1.8 above.

## Dependency

For Maven users, add the following dependency in your project's pom.xml.

```
<dependency>
    <groupId>org.mongoflink</groupId>
    <artifactId>mongo-flink</artifactId>
    <version>0.1</version>
</dependency>
```

MongoFlink heavily relies on Flink connector interfaces, but Flink interfaces may not have good cross version
compatibility, thus it's recommended to choose the version of MongoFlink that matches the version of Flink
in your project.

| version | flink version |
| ------- | ------------- |
| 0.1 | 1.13.1 |

In case there's no version that fits your need, it's recommended to build your own one. See [Build from source]
section below.

## Code

Use MongoSink in your Flink application.

```java
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // non-transactional sink with a flush strategy of 1000 documents or 10 seconds
    Properties properties = new Properties();
    properties.setProperty(MongoOptions.SINK_TRANSACTION_ENABLED, "false");
    properties.setProperty(MongoOptions.SINK_FLUSH_ON_CHECKPOINT, "false");
    properties.setProperty(MongoOptions.SINK_FLUSH_SIZE, String.valueOf(1_000L));
    properties.setProperty(MongoOptions.SINK_FLUSH_INTERVAL, String.valueOf(10_000L));

    env.addSource(...)
       .sinkTo(new MongoSink<>("mongodb://user:password@127.0.0.1:27017", "mydb", "mycollection",
                               new StringDocumentSerializer(), properties));

    env.execute();
```

# Configuration

MongoFlink can be configured using properties.

## MongoSink

| key | description | default value |
| --- | ----------- | ------------- |
| sink.transaction.enable | Whether use transactions in MongoSink (requires MongoDB 4.2+). | false |
| sink.flush.on-checkpoint | Whether flush the buffered documents on checkpoint barriers. | false |
| sink.flush.size | Max buffered documents before flush. Only valid when `sink.flush.on-checkpoint` is `false`. | 1000 |
| sink.flush.interval | Flush interval in milliseconds. Only valid when `sink.flush.on-checkpoint` is `false`. | 30000 |

# Build from source

Checkout the project, and use Maven to build the project locally.

```
$ mvn verify
```

# Contribute to the project

## Report an issue
Welcome to file an issue if you need help on adopting MongoFlink. Please describe your environment in the issue
(e.g. what MongoDB/Flink version you're using).

## Contribute code
Welcome to open pull requests if you want to improve MongoFlink. Please state the purpose (and the design if it's a big
feature) for better review experience.
