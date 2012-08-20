Storm HBase connector
=============

This is a HBase connector for Storm (https://github.com/nathanmarz/storm/). 

It provides two Bolt implementations:

* <b>HBaseBolt</b>: A bolt for transforming Tuples into Put requests and sending them to a HBase table. Works in both single and batch mode. In single mode each Put is sent straight to HBase and therefore requires a RPC for each. In batch mode HBase's client-side buffer is enabled which buffers Put requests until it is full, at which point all of the Puts are flushed to HBase in a single RPC. Batch mode is enabled by default and is recommended for high throughput streams.

* <b>HBaseCountersBolt</b>: An extension to HBaseBolt which transforms Tuples into HBase counter Increment requests. This is useful for storm topologies that collect statistics. Please note that this is a non-transactional bolt. Based on Storm's guaranteed message processing mechanism there is a chance of over-counting if tuples fail after updating the HBase counter and before they are successfully acked and are replayed.

Both implementations are generic and configurable. The TupleTableConfig class is used to configure the Bolts by storing the following configurable attributes:

* HBase table name
* The Tuple field to be used as the row key
* The Tuple field to be used as the rows timestamp (optional)
* Whether to enable HBase's client-side write buffer (batch mode)
* A map of column families to column qualifiers. The column qualifier names should correspond to the Tuple field names to be put into the table

Counters
-------------

By default the HBaseCountersBolt will use the Tuple output fields value to set the column qualifier name. For example given the following Tuples:

	Tuple1 = shorturl:http://bit.ly/LsaBa, date:20120816
	Tuple2 = shorturl:http://bit.ly/LsaBa, date:20120816
	Tuple3 = shorturl:http://bit.ly/LsaBa, date:20120816

And the TupleTableConfig configuration:

	Rowkey = shorturl
	CF = data, CQ = date

You will get the following counter in your HBase table:

	ROW					CF		CQ			COUNTER
	http://bit.ly/LsaBa	data	20120816	3

However, if you specify a CQ in the configuration that doesn't exist in the Tuple, for example 'clicks', the given CQ name is used for the counter:

	ROW					CF		CQ		COUNTER
	http://bit.ly/LsaBa	data	clicks	3

Build
-------------

	$ mvn install

---------------------------------------

Running the example topologies
-------------

The example topologies are based on the URL shortener project (Hush), taken from the HBase: The Definitive Guide book (http://shop.oreilly.com/product/0636920014348.do)

To build a jar with all of the required dependencies for running the examples:
	
	$ mvn assembly:assembly


Create the HBase table (assumes you have HBase installed and configured)

	create 'shorturl', {NAME => 'data', VERSIONS => 3}, {NAME => 'daily', VERSION => 1, TTL => 604800}

Both topologies use the same Spout, which outputs at random one of the following tuples:
	
	shorturl					url								name		date
	"http://bit.ly/ZK6t"		"www.arsenal.com/home"			"kinley"	"20120816"
	"http://bit.ly/LsaBa"		"www.baltimoreravens.com/"		"kinley"	"20120816"
	"http://bit.ly/2VL7eA"		"www.49ers.com/"				"kinley"	"20120816"
	"http://bit.ly/9ZJhuY"		"www.buccaneers.com/index.html"	"kinley"	"20120816"
	"http://atmlb.com/7NG4sm"	"baltimore.orioles.mlb.com/"	"kinley"	"20120816"

### Running the Put example

	java -cp target/storm-hbase-[version]-jar-with-dependencies.jar:/path/to/hbase/conf backtype.storm.contrib.hbase.example.HBaseExampleTopology

The /path/to/hbase/conf directory should contain your hbase-site.xml

With the following TupleTableConfig configuration:

	TupleTableConfig config = new TupleTableConfig("shorturl", "shortid");
	config.setBatch(false);
	config.addColumn("data", "url");
	config.addColumn("data", "user");
	config.addColumn("data", "date");

Your 'shorturl' table should look something like this:

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

### Running the Increment example

	java -cp target/storm-hbase-[version]-jar-with-dependencies.jar:/path/to/hbase/conf backtype.storm.contrib.hbase.example.HBaseCountersExampleTopology

The /path/to/hbase/conf directory should contain your hbase-site.xml

With the following TupleTableConfig configuration:

	TupleTableConfig config = new TupleTableConfig("shorturl", "shortid");
	config.setBatch(false);
	config.addColumn("data", "clicks");
	config.addColumn("daily", "date");

Your 'shorturl' table should look something like this:

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
	
---------------------------------------

Coming soon
-------------

* <b>HBaseCountersBatchBolt</b>: Another version of HBaseCountersBolt but for transactional topologies. This will be a committing batch bolt that will store transaction state alongside the counter in HBase to avoid over-counting due to failed, and subsequently replayed batches of tuples. See https://github.com/nathanmarz/storm/wiki/Transactional-topologies for more information on transactional topologies.
