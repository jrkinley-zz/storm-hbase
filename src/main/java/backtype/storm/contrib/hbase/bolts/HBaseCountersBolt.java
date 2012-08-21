package backtype.storm.contrib.hbase.bolts;

import java.io.IOException;

import backtype.storm.contrib.hbase.utils.TupleTableConfig;
import backtype.storm.tuple.Tuple;

/**
 * A Storm bolt for incrementing counters in HBase
 * <p>
 * <strong>Note: </strong>this is a non-transactional bolt. Based on Storm's
 * guaranteed message processing mechanism there is a chance of over-counting if
 * tuples fail after updating the HBase counter and before they are successfully
 * acked and are subsequently replayed.
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
