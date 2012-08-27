package backtype.storm.contrib.hbase.examples;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

@SuppressWarnings("serial")
public class TestSpout implements IRichSpout {

  List<Values> values;
  SpoutOutputCollector _collector;

  @SuppressWarnings("rawtypes")
  @Override
  public void open(Map conf, TopologyContext context,
      SpoutOutputCollector collector) {
    this._collector = collector;

    values = new ArrayList<Values>();
    values.add(new Values("http://bit.ly/ZK6t", "www.arsenal.com/home", "kinley", "20120816"));
    values.add(new Values("http://bit.ly/LsaBa", "www.baltimoreravens.com/", "kinley", "20120816"));
    values.add(new Values("http://bit.ly/2VL7eA", "www.49ers.com/", "kinley", "20120816"));
    values.add(new Values("http://bit.ly/9ZJhuY", "www.buccaneers.com/index.html", "kinley", "20120816"));
    values.add(new Values("http://atmlb.com/7NG4sm", "baltimore.orioles.mlb.com/", "kinley", "20120816"));
  }

  @Override
  public void close() {
  }

  @Override
  public void activate() {
  }

  @Override
  public void deactivate() {
  }

  @Override
  public void nextTuple() {
    int rand = (int) (Math.random() * 1000);
    _collector.emit(values.get(rand % values.size()));
  }

  @Override
  public void ack(Object msgId) {
  }

  @Override
  public void fail(Object msgId) {
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("shortid", "url", "user", "date"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }

}
