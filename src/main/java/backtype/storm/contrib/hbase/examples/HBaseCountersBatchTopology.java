package backtype.storm.contrib.hbase.examples;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.contrib.hbase.bolts.HBaseCountersBatchBolt;
import backtype.storm.contrib.hbase.utils.TupleTableConfig;
import backtype.storm.testing.MemoryTransactionalSpout;
import backtype.storm.transactional.TransactionalTopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * An example transactional topology that uses the
 * {@link HBaseCountersBatchBolt} to increment the following counters in a HBase
 * table:
 * <ul>
 * <li>cf:'data' cq:'clicks'
 * <li>cf:'daily' cq:'YYYYMMDD'
 * </ul>
 * The example assumes that the following table exists in HBase:<br>
 * <tt>create 'shorturl', {NAME => 'data', VERSIONS => 3}, {NAME => 'daily', VERSION => 1, TTL => 604800}</tt>
 */
public class HBaseCountersBatchTopology {

  @SuppressWarnings("serial")
  final static Map<Integer, List<List<Object>>> values = new HashMap<Integer, List<List<Object>>>() {{
    put(0, new ArrayList<List<Object>>() {{
      add(new Values("http://bit.ly/ZK6t", "www.arsenal.com/home", "kinley", "20120816"));
      add(new Values("http://bit.ly/LsaBa", "www.baltimoreravens.com/", "kinley", "20120816"));
      add(new Values("http://bit.ly/2VL7eA", "www.49ers.com/", "kinley", "20120816"));
      add(new Values("http://bit.ly/9ZJhuY", "www.buccaneers.com/index.html", "kinley", "20120816"));
      add(new Values("http://atmlb.com/7NG4sm", "baltimore.orioles.mlb.com/", "kinley", "20120816"));
    }});
    put(1, new ArrayList<List<Object>>() {{
      add(new Values("http://bit.ly/ZK6t", "www.arsenal.com/home", "kinley", "20120816"));
      add(new Values("http://bit.ly/ZK6t", "www.arsenal.com/home", "kinley", "20120816"));
      add(new Values("http://bit.ly/ZK6t", "www.arsenal.com/home", "kinley", "20120816"));
    }});
    put(2, new ArrayList<List<Object>>() {{
      add(new Values("http://bit.ly/LsaBa", "www.baltimoreravens.com/", "kinley", "20120816"));
      add(new Values("http://bit.ly/LsaBa", "www.baltimoreravens.com/", "kinley", "20120816"));
      add(new Values("http://bit.ly/LsaBa", "www.baltimoreravens.com/", "kinley", "20120816"));
    }});
  }};
  
  /**
   * @param args
   * @throws InterruptedException 
   */
  public static void main(String[] args) throws InterruptedException {
    // Add transactional spout
    MemoryTransactionalSpout spout = new MemoryTransactionalSpout(values,
        new Fields("shortid", "url", "user", "date"), 3);

    TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder(
        "shorturl-count", "spout", spout, 2);

    // Build TupleTableConifg
    TupleTableConfig ttConfig = new TupleTableConfig("shorturl", "shortid");
    ttConfig.setBatch(false);
    ttConfig.addColumn("data", "clicks");
    ttConfig.addColumn("daily", "date");

    builder.setBolt("hbase-counters", new HBaseCountersBatchBolt(ttConfig), 2)
        .fieldsGrouping("spout", new Fields("shortid"));

    LocalCluster cluster = new LocalCluster();

    Config stormConfig = new Config();
    stormConfig.setDebug(true);
    stormConfig.setMaxSpoutPending(3);

    cluster.submitTopology("hbase-example", stormConfig,
        builder.buildTopology());

    Thread.sleep(3000);
    cluster.shutdown();
  }

}
