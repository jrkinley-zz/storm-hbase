package backtype.storm.contrib.hbase.trident;

import java.util.Map;

import storm.trident.state.State;
import storm.trident.state.StateFactory;
import backtype.storm.contrib.hbase.utils.TridentConfig;
import backtype.storm.task.IMetricsContext;

/**
 * Factory for creating {@link HBaseValueState}
 */
@SuppressWarnings({ "serial", "rawtypes" })
public class HBaseValueFactory implements StateFactory {
  private TridentConfig _conf;

  public HBaseValueFactory(final TridentConfig conf) {
    this._conf = conf;
  }

  /** {@inheritDoc} */
  @Override
  public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
    return new HBaseValueState(_conf);
  }
}
