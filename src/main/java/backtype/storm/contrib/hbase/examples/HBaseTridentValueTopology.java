package backtype.storm.contrib.hbase.examples;

import java.util.List;

import storm.trident.TridentTopology;
import storm.trident.testing.FixedBatchSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.contrib.hbase.trident.HBaseValueFactory;
import backtype.storm.contrib.hbase.trident.HBaseValueState;
import backtype.storm.contrib.hbase.trident.HBaseValueUpdater;
import backtype.storm.contrib.hbase.utils.TridentConfig;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 * An example Storm Trident topology that uses the {@link HBaseValueState} to
 * insert a stream of shortened URL's into a HBase table called 'shorturl'.
 * <p>
 * Assumes the HBase table has been created.<br>
 * <tt>create 'shorturl', {NAME => 'data', VERSIONS => 3}, {NAME => 'daily', VERSION => 1, TTL => 604800}</tt>
 */
public class HBaseTridentValueTopology {
  /**
   * @param args
   * @throws InterruptedException
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static void main(String[] args) throws InterruptedException {
    List<Object> v0 = HBaseCountersBatchTopology.values.get(0).get(0);
    List<Object> v1 = HBaseCountersBatchTopology.values.get(0).get(1);
    List<Object> v2 = HBaseCountersBatchTopology.values.get(0).get(2);
    List<Object> v3 = HBaseCountersBatchTopology.values.get(0).get(3);
    List<Object> v4 = HBaseCountersBatchTopology.values.get(0).get(4);

    FixedBatchSpout spout = new FixedBatchSpout(new Fields("shortid", "url",
        "user", "date"), 3, v0, v1, v2, v3, v4);
    spout.setCycle(true);

    // Trident updater
    TridentConfig updateConfig = new TridentConfig("shorturl", "shortid");
    updateConfig.setBatch(false);
    updateConfig.addColumn("data", "url");
    updateConfig.addColumn("data", "user");
    updateConfig.addColumn("data", "date");

    TridentTopology topology = new TridentTopology();
    topology.newStream("shorturls", spout).partitionPersist(
        new HBaseValueFactory(updateConfig),
        new Fields("shortid", "url", "user", "date"), new HBaseValueUpdater());

    Config conf = new Config();
    LocalCluster cluster = new LocalCluster();

    cluster.submitTopology("hbase-trident-updater", conf, topology.build());
    Utils.sleep(10000);
    cluster.shutdown();
  }
}
