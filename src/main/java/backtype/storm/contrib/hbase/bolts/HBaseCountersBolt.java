package backtype.storm.contrib.hbase.bolts;

import java.io.IOException;

import backtype.storm.contrib.hbase.utils.TupleTableConfig;
import backtype.storm.tuple.Tuple;

/**
 * A Storm bolt for incrementing counters in HBase
 * 
 * @see HBaseBolt
 */
@SuppressWarnings("serial")
public class HBaseCountersBolt extends HBaseBolt {

  private static final long DEFAULT_INCREMENT = 1L;

  public HBaseCountersBolt(TupleTableConfig conf) {
    super(conf);
  }

  /** {@inheritDoc} */
  @Override
  public void execute(Tuple input) {
    try {
      this.connector.increment(conf.getIncrementFromTuple(input,
          DEFAULT_INCREMENT));
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    if (this.autoAck) {
      this.collector.ack(input);
    }
  }

}
