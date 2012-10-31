# storm-hbase connector

<b>storm-hbase</b> is a HBase connector for Storm (https://github.com/nathanmarz/storm/). It enables huge amounts of streaming data, and stateful representations of streaming data, to be persisted in HBase - the scalable, distributed, big data store.

The connector provides a number of Bolt and Trident State implementations. It can be used for simple Storm to HBase integration and in more complex architectures, such as the "Lambda Architecture" as described by Nathan Marz:

* http://www.slideshare.net/nathanmarz/runaway-complexity-in-big-data-and-a-plan-to-stop-it
* http://www.manning.com/marz/

In this type of architecture the connector can be used to persist a precomputed realtime view in HBase. Then tools like Storm (DRPC) and Cloudera Impala can be used to query and merge the realtime and batch computed views to present the most accurate and up-to-date information to the users.

Further documentation and example topologies can be found on these wiki pages:

* [HBase Storm Bolts](https://github.com/jrkinley/storm-hbase/wiki/HBase-Storm-Bolts)
* [HBase Trident](https://github.com/jrkinley/storm-hbase/wiki/HBase-Trident)