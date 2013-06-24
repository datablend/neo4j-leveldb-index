# Neo4j 2.0 LevelDB Index Provider

Implements a Schema Index Provider for Neo4j 2.0, label based indexes using [LevelDB](https://github.com/dain/leveldb), which is a high performance, persistent key-value store.

It also supports snapshots which are required by a Schema Index Provider for repeatable reads.

`mvn clean install`

That will create a zip-file: `target/leveldb-index-1.0-provider.zip` whose content you have to put in Neo4j's classpath.
