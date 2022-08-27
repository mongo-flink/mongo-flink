# MongoFlink
MongoFlink is a connector between MongoDB and Apache Flink. It acts as a Flink sink (and an experimental Flink bounded
source), and provides transaction mode(which ensures exactly-once semantics) for MongoDB 4.2 above, and non-transaction
mode for MongoDB 3.0 above.

MongoFlink is in its early phase, and any use, feedback or contribution is welcome!

# Start to use
## Prerequisite

- Flink 1.12 above. MongoFlink is built on top of the new sink API added in Flink 1.12.0.
- MongoDB 3.0 above. The official MongoDB Java driver supports 3.0 above.
- JDK 1.8 above.

## Dependency

For Maven users, add the following dependency in your project's pom.xml.

```
<dependency>
	<groupId>org.mongoflink</groupId>
	<artifactId>mongo-flink</artifactId>
	<version>0.3</version>
</dependency>
```

MongoFlink heavily relies on Flink connector interfaces, but Flink interfaces may not have good cross version
compatibility, thus it's recommended to choose the version of MongoFlink that matches the version of Flink
in your project.

With regard to MongoDB compatibility, please refer to [MongoDB's docs about the Java driver](https://www.mongodb.com/docs/drivers/java/sync/current/compatibility/).

| version | flink version | mongodb driver version |
| ------- | ------------- |------------------------|
| 0.1 | 1.13.x | 4.2.x                  |
| 0.2 | 1.14.x | 4.4.x                  |
| 0.3 | 1.15.x | 4.4.x                  |

In case there's no version that fits your need, it's recommended to build your own one. See [Build from source]
section below.

## Code

Use MongoSink in your Flink DataStream application.

```java
	StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

	// non-transactional sink with a flush strategy of 1000 documents or 10 seconds
	MongoConnectorOptions options = MongoConnectorOptions.builder()
		.withDatabase("my_db")
		.withCollection("my_collection")
		.withConnectString("mongodb://user:password@127.0.0.1:27017")
		.withTransactionEnable(false)
		.withFlushOnCheckpoint(false)
		.withFlushSize(1_000L)
		.withFlushInterval(Duration.ofSeconds(10))
		.build();

	env.addSource(...)
	.sinkTo(new MongoSink<>(new StringDocumentSerializer(), options));

	env.execute();
```

Use MongoSink in your Flink Table/SQL application.

```java
	TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

	env.executeSql("create table tbl_user_gold (" +
			"    user_id long," +
			"    gold long," +
			"    PRIMARY key(user_id) NOT ENFORCED" +
			") with (" +
			"    'connector'='mongo'," +
			"    'connect_string' = 'mongodb://user:password@127.0.0.1:27017'," +
			"    'database' = 'mydb'," +
			"    'collection' = 'user_gold'" +
			")"

	Table userGold = env.executeQuery("select * from tbl_user_gold");
	);
```

# Configuration

MongoFlink can be configured using `MongoConnectorOptions` or properties (deprecated).

## MongoSink

| option                                  | properties key                              | description                                                                                    | default value |
|-----------------------------------------|---------------------------------------------|------------------------------------------------------------------------------------------------|--------------|
| MongoConnectorOptions.transactionEnable | sink.transaction.enable                     | Whether to use transactions in MongoSink (requires MongoDB 4.2+).                              | false        |
| MongoConnectorOptions.flushOnCheckpoint | sink.flush.on-checkpoint                    | Whether to flush the buffered documents on checkpoint barriers.                                | false        |
| MongoConnectorOptions.flushSize         | sink.flush.size                             | Max buffered documents before flush. Only valid when `sink.flush.on-checkpoint` is `false`.    | 1000         |
| MongoConnectorOptions.flushInterva      | sink.flush.interval                         | Flush interval in milliseconds. Only valid when `sink.flush.on-checkpoint` is `false`.         | 30000        |
| MongoConnectorOptions.maxInFlightFlushes | sink.max.in-flight.flushes                  | Max in-flight flushes before blocking the writer.                                              | 5            |
| MongoConnectorOptions.upsertEnable | (true if the table has primary keys)        | Whether to write documents via upsert mode.                                                    | false        |
| MongoConnectorOptions.upsertKey | (derived automatically from the primary key) | The primary keys for upsert. Only valid in upsert mode. Keys are in csv format for properties. | []           |


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
