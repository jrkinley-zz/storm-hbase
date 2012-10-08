package backtype.storm.contrib.hbase.examples;

import java.util.List;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.state.StateFactory;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.contrib.hbase.trident.HBaseAggregateState;
import backtype.storm.contrib.hbase.utils.TridentConfig;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * An example Storm Trident topology that uses {@link HBaseAggregateState} for
 * stateful stream processing.
 * <p>
 * This example persists idempotent counts in HBase for the number of times a
 * shortened URL has been seen in the stream for each day, week, and month.
 * <p>
 * Assumes the HBase table has been created.<br>
 * <tt>create 'shorturl', {NAME => 'data', VERSIONS => 3}, 
 * {NAME => 'daily', VERSION => 1, TTL => 604800}, 
 * {NAME => 'weekly', VERSION => 1, TTL => 2678400}, 
 * {NAME => 'monthly', VERSION => 1, TTL => 31536000}</tt>
 */
public class HBaseTridentAggregateTopology {

  /**
   * Partitions a tuple into three to represent daily, weekly, and monthly stats
   * <p>
   * For example, when passed the following tuple:<br>
   * {shortid:http://bit.ly/ZK6t, date:20120816}
   * <p>
   * The function will output the following three tuples:<br>
   * {shortid:http://bit.ly/ZK6t, date:20120816, cf:daily, cq:20120816}<br>
   * {shortid:http://bit.ly/ZK6t, date:20120816, cf:weekly, cq:201233}<br>
   * {shortid:http://bit.ly/ZK6t, date:20120816, cf:monthly, cq:201208}
   */
  @SuppressWarnings("serial")
  static class DatePartitionFunction extends BaseFunction {
    final String cfStatsDaily = "daily";
    final String cfStatsWeekly = "weekly";
    final String cfStatsMonthly = "monthly";
    final static transient DateTimeFormatter dtf = DateTimeFormat
        .forPattern("YYYYMMdd");

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
      String monthly = tuple.getString(1).substring(0, 6);
      Integer week = dtf.parseDateTime(tuple.getString(1)).getWeekOfWeekyear();
      String weekly = tuple.getString(1).substring(0, 4)
          .concat(week.toString());

      collector.emit(new Values(cfStatsDaily, tuple.getString(1)));
      collector.emit(new Values(cfStatsMonthly, monthly));
      collector.emit(new Values(cfStatsWeekly, weekly));
    }
  }

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
    List<Object> v5 = HBaseCountersBatchTopology.values.get(1).get(0);
    List<Object> v6 = HBaseCountersBatchTopology.values.get(1).get(1);
    List<Object> v7 = HBaseCountersBatchTopology.values.get(1).get(2);
    List<Object> v8 = HBaseCountersBatchTopology.values.get(2).get(0);
    List<Object> v9 = HBaseCountersBatchTopology.values.get(2).get(1);
    List<Object> v10 = HBaseCountersBatchTopology.values.get(2).get(2);

    HBaseCountersBatchTopology.values.values();

    FixedBatchSpout spout = new FixedBatchSpout(new Fields("shortid", "url",
        "user", "date"), 3, v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10);
    spout.setCycle(false);

    TridentConfig config = new TridentConfig("shorturl", "shortid");
    config.setBatch(false);

    StateFactory state = HBaseAggregateState.transactional(config);

    TridentTopology topology = new TridentTopology();
    topology
        .newStream("spout", spout)
        .each(new Fields("shortid", "date"), new DatePartitionFunction(),
            new Fields("cf", "cq")).project(new Fields("shortid", "cf", "cq"))
        .groupBy(new Fields("shortid", "cf", "cq"))
        .persistentAggregate(state, new Count(), new Fields("count"));

    Config conf = new Config();
    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("hbase-trident-aggregate", conf, topology.build());

    Utils.sleep(5000);
    cluster.shutdown();
  }
}
