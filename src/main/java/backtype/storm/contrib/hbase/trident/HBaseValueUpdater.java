package backtype.storm.contrib.hbase.trident;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

/**
 * Storm Trident state updater for {@link HBaseValueState}
 */
@SuppressWarnings("serial")
public class HBaseValueUpdater extends BaseStateUpdater<HBaseValueState> {

  /** {@inheritDoc} */
  @Override
  public void updateState(HBaseValueState state, List<TridentTuple> tuples,
      TridentCollector collector) {
    List<Put> puts = new ArrayList<Put>();
    for (TridentTuple t : tuples) {
      puts.add(state.getConf().getPutFromTridentTuple(t));
    }
    state.setValuesBulk(puts);
  }

}
