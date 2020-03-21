# Flink DynamoDB backed Filesystem

### Build

```
git clone https://github.com/mattczyz/flink-dynamodb-fs.git
cd flink-dynamodb-fs
mvn package
```

### Configuration

Copy `target/flink-dynamodb-fs-0.1.jar` into `flink/lib/`.

flink.yaml
```
fs.ddb.table.read.capacity: 5 // default 1
fs.ddb.table.write.capacity: 5 // default 1
```

To setup credentials follow "Using the Default Credential Provider Chain" under [Working with AWS Credentials](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html).

Scheme `ddb:///`.

The filesystem creates two DynamoDB tables [prefix]-meta and [prefix]-data where prefix is build from `state.checkpoints.dir`. 

e.g. `ddb:///namespace/checkpoints/` will result in creating `namespace-checkpoints-meta` and `namespace-checkpoints-data` tables.
