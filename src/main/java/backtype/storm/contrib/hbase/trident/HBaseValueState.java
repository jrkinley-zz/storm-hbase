package backtype.storm.contrib.hbase.trident;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;

import storm.trident.state.State;
import backtype.storm.contrib.hbase.utils.HTableConnector;
import backtype.storm.contrib.hbase.utils.TridentConfig;

/**
 * Storm Trident state implementation for putting and getting values from a
 * HBase table
 */
@SuppressWarnings("rawtypes")
public class HBaseValueState implements State {
  private static final Logger LOG = Logger.getLogger(HBaseValueState.class);

  private HTableConnector _connector;
  private TridentConfig _conf;

  public HBaseValueState(final TridentConfig conf) {
    this._conf = conf;
  }

  /** {@inheritDoc} */
  @Override
  public void beginCommit(Long txid) {
    LOG.debug("Beginning commit for tx " + txid);
    try {
      _connector = new HTableConnector(_conf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void commit(Long txid) {
    LOG.debug("Commit tx " + txid);
    _connector.close();
  }

  /**
   * Send the puts to HBase
   * 
   * @param puts
   */
  public void setValuesBulk(final List<Put> puts) {
    try {
      _connector.getTable().put(puts);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Retrieve gets from HBase
   * 
   * @param gets
   * @return List of HBase results from the the given gets
   */
  public List<Result> getValuesBulk(final List<Get> gets) {
    Result[] results;
    try {
      results = _connector.getTable().get(gets);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return Arrays.asList(results);
  }

  /**
   * @return the conf
   */
  public TridentConfig getConf() {
    return _conf;
  }
}
