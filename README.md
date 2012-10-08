Storm HBase connector
=============

This is a HBase connector for Storm (https://github.com/nathanmarz/storm/). 

It provides three Bolt implementations:

* <b><tt>HBaseBolt</tt></b>: A Bolt for transforming Tuples into Put requests and sending them to a HBase table. Works in both single and batch mode. In single mode each Put is sent straight to HBase and therefore requires a RPC for each request. In batch mode HBase's client-side write buffer is enabled which buffers Put requests until it is full, at which point all of the Puts are flushed to HBase in a single RPC. Batch mode is enabled by default and is recommended for high throughput streams.

* <b><tt>HBaseCountersBolt</tt></b>: A Bolt for transforming Tuples into HBase counter Increment requests. This is useful for storm topologies that collect statistics. Please note that this is a non-transactional bolt and therefore cannot achieve exactly-once processing semantics. Based on Storm's guaranteed message processing mechanism there is a chance of over-counting if Tuples fail after updating the HBase counter and before they are successfully acked in Storm, and are subsequently replayed.

* <b><tt>HBaseCounterBatchBolt</tt></b>: A transactional version of <tt>HBaseCountersBolt</tt> for incrementing idempotent counters in HBase. This Bolt is intended for use in transactional topologies, which enable you to achieve exactly once processing semantics (https://github.com/nathanmarz/storm/wiki/Transactional-topologies). It stores the counter and the latest transaction ID (txid) together in the HBase table. The counter is only incremented if the txid in the table is different from the txid of the Tuple being processed. E.g:
If the txids are different, because of Storm's strong ordering of transactions, we know that the current Tuple hasn't been represented in the counter, so the counter is incremented and its latest txid updated.
If the txids are the same, we know that the current Tuple is represented in the counter so it is skipped. The Tuple must have failed after previously incrementing the counter but before reporting success back to Storm, so it was replayed.

All implementations are generic and configurable. The <tt>TupleTableConfig</tt> class is used to configure the Bolts with the following attributes:

* <tt>tableName</tt>: The HBase table name to connect to
* <tt>tupleRowKeyField</tt>: The Tuple field to be used as the row key
* <tt>tupleTimestampField</tt>: The Tuple field to be used as the rows timestamp <i>(optional)</i>
* <tt>batch</tt>: Whether to enable HBase's client-side write buffer (batch mode) <i>(enabled by default)</i>
* <tt>writeBufferSize</tt>: The size of the client-side write buffer <i>(optional) (overrides the value in <tt>hbase-site.xml</tt>, 2097152 bytes by default)</i>
* <tt>writeToWAL</tt>: Whether to write to HBase's edit log (WAL) <i>(enabled by default)</i>
* <tt>columnFamilies</tt>: A map of column families to column qualifiers. The column qualifier names should correspond to the Tuple field names to be put into the table


Counters
-------------

By default <tt>HBaseCountersBolt</tt> and <tt>HBaseCountersBatchBolt</tt> use the Tuple field value to set the Increment request column qualifier name. For example, given the following Tuples:

	Tuple1 = shorturl:http://bit.ly/LsaBa, date:20120816
	Tuple2 = shorturl:http://bit.ly/LsaBa, date:20120816
	Tuple3 = shorturl:http://bit.ly/LsaBa, date:20120816

And the <tt>TupleTableConfig</tt> configuration:

	Rowkey = "shorturl"
	CF = "data", CQ = "date"

You will get the following counter in your HBase table:

	ROW						CF		CQ			COUNTER
	http://bit.ly/LsaBa		data	20120816	3

However, if you specify a CQ in the configuration that doesn't exist in the Tuple, for example "clicks", the given CQ name is used for the counter:

	ROW						CF		CQ		COUNTER
	http://bit.ly/LsaBa		data	clicks	3


Build
-------------

	$ mvn install


---------------------------------------


Running the example topologies
-------------

The example topologies are based on the URL shortener project (Hush), taken from the <i>HBase: The Definitive Guide</i> book (http://shop.oreilly.com/product/0636920014348.do):

* <b><tt>HBaseExampleTopology</tt></b>: A non-transactional topology that demonstrates how to use <tt>HBaseBolt</tt> to put data from Storm into HBase.
* <b><tt>HBaseCountersExampleTopology</tt></b>: A non-transactional topology that demonstrates how to use <tt>HBaseCountersBolt</tt> to increment counters in HBase.
* <b><tt>HBaseCountersBatchTopology</tt></b>: A transactional topology that demonstrates how to use <tt>HBaseCounterBatchBolt</tt> to increment idempotent counters in HBase.

To build a jar with all of the required dependencies for running the examples:

	$ mvn assembly:assembly


Create the HBase table (assumes you have HBase installed and configured):

	create 'shorturl', {NAME => 'data', VERSIONS => 3}, {NAME => 'daily', VERSION => 1, TTL => 604800}

The <tt>HBaseExampleTopology</tt> and <tt>HBaseCountersExampleTopology</tt> topologies use the same Spout, which outputs at random one of the following Tuples:
  
	shorturl					url								name		date
	"http://bit.ly/ZK6t"		"www.arsenal.com/home"			"kinley"	"20120816"
	"http://bit.ly/LsaBa"		"www.baltimoreravens.com/"		"kinley"	"20120816"
	"http://bit.ly/2VL7eA"		"www.49ers.com/"				"kinley"	"20120816"
	"http://bit.ly/9ZJhuY"		"www.buccaneers.com/index.html"	"kinley"	"20120816"
	"http://atmlb.com/7NG4sm"	"baltimore.orioles.mlb.com/"	"kinley"	"20120816"

The <tt>HBaseCountersBatchTopology</tt> uses the <tt>backtype.storm.testing.MemoryTransactionalSpout</tt> to output batches of the Tuples described above.


### Running <tt>HBaseExampleTopology</tt>

	java -cp target/storm-hbase-[version]-jar-with-dependencies.jar:/path/to/hbase/conf backtype.storm.contrib.hbase.example.HBaseExampleTopology

The <tt>/path/to/hbase/conf</tt> directory should contain your <tt>hbase-site.xml</tt>

With the following <tt>TupleTableConfig</tt> configuration:

	TupleTableConfig config = new TupleTableConfig("shorturl", "shortid");
	config.setBatch(false);
	config.addColumn("data", "url");
	config.addColumn("data", "user");
	config.addColumn("data", "date");

Your "shorturl" table should look something like this:

	ROW							COLUMN+CELL
	http://atmlb.com/7NG4sm		column=data:date, timestamp=1345115796927, value=20120816
	http://atmlb.com/7NG4sm		column=data:url, timestamp=1345115796927, value=baltimore.orioles.mlb.com/
	http://atmlb.com/7NG4sm		column=data:user, timestamp=1345115796927, value=kinley
	http://bit.ly/2VL7eA		column=data:date, timestamp=1345115796935, value=20120816
	http://bit.ly/2VL7eA		column=data:url, timestamp=1345115796935, value=www.49ers.com/
	http://bit.ly/2VL7eA		column=data:user, timestamp=1345115796935, value=kinley
	http://bit.ly/9ZJhuY		column=data:date, timestamp=1345115796937, value=20120816
	http://bit.ly/9ZJhuY		column=data:url, timestamp=1345115796937, value=www.buccaneers.com/index.html
	http://bit.ly/9ZJhuY		column=data:user, timestamp=1345115796937, value=kinley
	http://bit.ly/LsaBa			column=data:date, timestamp=1345115796929, value=20120816
	http://bit.ly/LsaBa			column=data:url, timestamp=1345115796929, value=www.baltimoreravens.com/
	http://bit.ly/LsaBa			column=data:user, timestamp=1345115796929, value=kinley
	http://bit.ly/ZK6t			column=data:date, timestamp=1345115796930, value=20120816
	http://bit.ly/ZK6t			column=data:url, timestamp=1345115796930, value=www.arsenal.com/home
	http://bit.ly/ZK6t			column=data:user, timestamp=1345115796930, value=kinley


### Running <tt>HBaseCountersExampleTopology</tt>

	java -cp target/storm-hbase-[version]-jar-with-dependencies.jar:/path/to/hbase/conf backtype.storm.contrib.hbase.example.HBaseCountersExampleTopology

The <tt>/path/to/hbase/conf</tt> directory should contain your <tt>hbase-site.xml</tt>

With the following <tt>TupleTableConfig</tt> configuration:

	TupleTableConfig config = new TupleTableConfig("shorturl", "shortid");
	config.setBatch(false);
	config.addColumn("data", "clicks");
	config.addColumn("daily", "date");

Your "shorturl" table should look something like this:

	ROW							COLUMN+CELL
	http://atmlb.com/7NG4sm		column=daily:20120816, timestamp=1345115849682, value=\x00\x00\x00\x00\x00\x00\x05\x8C
	http://atmlb.com/7NG4sm		column=data:clicks, timestamp=1345115849682, value=\x00\x00\x00\x00\x00\x00\x05\x8C
	http://bit.ly/2VL7eA		column=daily:20120816, timestamp=1345115849679, value=\x00\x00\x00\x00\x00\x00\x05\xA1
	http://bit.ly/2VL7eA		column=data:clicks, timestamp=1345115849679, value=\x00\x00\x00\x00\x00\x00\x05\xA1
	http://bit.ly/9ZJhuY		column=daily:20120816, timestamp=1345115849665, value=\x00\x00\x00\x00\x00\x00\x05\xFC
	http://bit.ly/9ZJhuY		column=data:clicks, timestamp=1345115849665, value=\x00\x00\x00\x00\x00\x00\x05\xFC
	http://bit.ly/LsaBa			column=daily:20120816, timestamp=1345115849674, value=\x00\x00\x00\x00\x00\x00\x05\x94
	http://bit.ly/LsaBa			column=data:clicks, timestamp=1345115849674, value=\x00\x00\x00\x00\x00\x00\x05\x94
	http://bit.ly/ZK6t			column=daily:20120816, timestamp=1345115849678, value=\x00\x00\x00\x00\x00\x00\x05v
	http://bit.ly/ZK6t			column=data:clicks, timestamp=1345115849678, value=\x00\x00\x00\x00\x00\x00\x05v

To see the counter value, run the following in the HBase shell:

	$ get_counter 'shorturl', 'http://atmlb.com/7NG4sm', 'daily:20120816'
	COUNTER VALUE = 1420

	$ get_counter 'shorturl', 'http://atmlb.com/7NG4sm', 'data:clicks'
	COUNTER VALUE = 1420


### Running <tt>HBaseCountersBatchTopology</tt>

	java -cp target/storm-hbase-[version]-jar-with-dependencies.jar:/path/to/hbase/conf backtype.storm.contrib.hbase.example.HBaseCountersBatchTopology

The <tt>/path/to/hbase/conf</tt> directory should contain your <tt>hbase-site.xml</tt>

With the following <tt>TupleTableConfig</tt> configuration:

	TupleTableConfig config = new TupleTableConfig("shorturl", "shortid");
	config.setBatch(false);
	config.addColumn("data", "clicks");
	config.addColumn("daily", "date");

Your "shorturl" table should look something like this:

	ROW							COLUMN+CELL
	http://atmlb.com/7NG4sm		column=daily:20120816, timestamp=1345935584520, value=\x00\x00\x00\x00\x00\x00\x00\x01
	http://atmlb.com/7NG4sm		column=daily:20120816_txid, timestamp=1345935584523, value=\x02
	http://atmlb.com/7NG4sm		column=data:clicks, timestamp=1345935584528, value=\x00\x00\x00\x00\x00\x00\x00\x01
	http://atmlb.com/7NG4sm		column=data:clicks_txid, timestamp=1345935584531, value=\x02
	http://bit.ly/2VL7eA		column=daily:20120816, timestamp=1345935584434, value=\x00\x00\x00\x00\x00\x00\x00\x01
	http://bit.ly/2VL7eA		column=daily:20120816_txid, timestamp=1345935584439, value=\x01
	http://bit.ly/2VL7eA		column=data:clicks, timestamp=1345935584444, value=\x00\x00\x00\x00\x00\x00\x00\x01
	http://bit.ly/2VL7eA		column=data:clicks_txid, timestamp=1345935584446, value=\x01
	http://bit.ly/9ZJhuY		column=daily:20120816, timestamp=1345935584535, value=\x00\x00\x00\x00\x00\x00\x00\x01
	http://bit.ly/9ZJhuY		column=daily:20120816_txid, timestamp=1345935584538, value=\x02
	http://bit.ly/9ZJhuY		column=data:clicks, timestamp=1345935584542, value=\x00\x00\x00\x00\x00\x00\x00\x01
	http://bit.ly/9ZJhuY		column=data:clicks_txid, timestamp=1345935584544, value=\x02
	http://bit.ly/LsaBa			column=daily:20120816, timestamp=1345935584450, value=\x00\x00\x00\x00\x00\x00\x00\x04
	http://bit.ly/LsaBa			column=daily:20120816_txid, timestamp=1345935584452, value=\x01
	http://bit.ly/LsaBa			column=data:clicks, timestamp=1345935584456, value=\x00\x00\x00\x00\x00\x00\x00\x04
	http://bit.ly/LsaBa			column=data:clicks_txid, timestamp=1345935584458, value=\x01
	http://bit.ly/ZK6t			column=daily:20120816, timestamp=1345935584462, value=\x00\x00\x00\x00\x00\x00\x00\x04
	http://bit.ly/ZK6t			column=daily:20120816_txid, timestamp=1345935584465, value=\x01
	http://bit.ly/ZK6t			column=data:clicks, timestamp=1345935584469, value=\x00\x00\x00\x00\x00\x00\x00\x04
	http://bit.ly/ZK6t			column=data:clicks_txid, timestamp=1345935584471, value=\x01
