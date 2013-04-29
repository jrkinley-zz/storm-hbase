# storm-hbase connector

<b>storm-hbase</b> is a HBase connector for Storm (https://github.com/nathanmarz/storm/). It enables huge amounts of streaming data, and stateful representations of streaming data, to be persisted in HBase - the scalable, distributed, big data store.

The connector provides a number of Bolt and Trident State implementations. It can be used for simple Storm to HBase integration and in more complex architectures, such as the "Lambda Architecture":

* http://jameskinley.tumblr.com/post/37398560534/the-lambda-architecture-principles-for-architecting

Further documentation and example topologies can be found on these wiki pages:

* [HBase Storm Bolts](https://github.com/jrkinley/storm-hbase/wiki/HBase-Storm-Bolts)
* [HBase Trident](https://github.com/jrkinley/storm-hbase/wiki/HBase-Trident)