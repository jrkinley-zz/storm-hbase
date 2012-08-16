package backtype.storm.contrib.hbase.example;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.contrib.hbase.bolts.HBaseCountersBolt;
import backtype.storm.contrib.hbase.utils.TupleTableConfig;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 * Example HBaseCountersBolt topology that increments the following counters in
 * the 'shorturl' table:
 * <ul>
 * <li>cf:'data' cq:'clicks'
 * <li>cf:'daily' cq:'YYYYMMDD'
 * </ul>
 * Assumes the HBase table has been created. E.g.
 * "create 'shorturl', {NAME => 'data', VERSIONS => 3}, {NAME => 'daily', VERSION => 1, TTL => 604800}"
 */
public class HBaseCountersExampleTopology {

  /**
   * @param args
   */
  public static void main(String[] args) {
    TopologyBuilder builder = new TopologyBuilder();

    // Add test spout
    builder.setSpout("spout", new TestSpout(), 1);

    // Build TupleTableConifg
    TupleTableConfig config = new TupleTableConfig("shorturl", "shortid");
    config.setBatch(false);
    /*
     * By default the HBaseCountersBolt will use the tuple output fields value
     * to set the CQ name. For example the 'date' output field exists in the
     * tuple and its value (e.g. "YYYYMMDD") will be used to set the counters
     * CQ. However, the 'clicks' output field does not exist in the tuple and in
     * this case the counters CQ will be set to the given name 'clicks'.
     */
    config.addColumn("data", "clicks");
    config.addColumn("daily", "date");

    // Add HBaseBolt
    builder.setBolt("hbase-counters", new HBaseCountersBolt(config), 1)
        .shuffleGrouping("spout");

    Config stormConf = new Config();
    stormConf.setDebug(true);

    LocalCluster cluster = new LocalCluster();
    cluster
        .submitTopology("hbase-example", stormConf, builder.createTopology());

    Utils.sleep(10000);
    cluster.shutdown();
  }

}
