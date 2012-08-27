package backtype.storm.contrib.hbase.bolts;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import backtype.storm.contrib.hbase.utils.HTableConnector;
import backtype.storm.contrib.hbase.utils.TupleTableConfig;
import backtype.storm.coordination.BatchBoltExecutor;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.ICommitter;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * A Storm bolt for incrementing idempotent counters in HBase
 * <p>
 * This bolt is intended for use in Storm transactional topologies, which enable
 * you to achieve exactly once processing semantics in a fully-accurate,
 * scalable, and fault-tolerant way:
 * <p>
 * https://github.com/nathanmarz/storm/wiki/Transactional-topologies
 * <p>
 * This bolt stores the counter and the latest transaction ID (txid) together in
 * the HBase table. The counter is only incremented if the txid in the table is
 * different from the txid of the Tuple being processed. E.g:
 * <ol>
 * <li>If the txids are different, because of Storm's strong ordering of
 * transactions, we know that the current Tuple hasn't been represented in the
 * counter, so the counter is incremented and its latest txid updated</li>
 * <li>If the txids are the same, we know that the current Tuple is represented
 * in the counter so it is skipped. The Tuple must have failed after previously
 * incrementing the counter but before reporting success back to Storm, so it
 * was replayed</li>
 * </ol>
 * 
 * @see BatchBoltExecutor
 * @see HTableConnector
 * @see TupleTableConfig
 */
@SuppressWarnings("serial")
public class HBaseCountersBatchBolt extends BaseTransactionalBolt implements
    ICommitter {

  private static final Logger LOG = Logger
      .getLogger(HBaseCountersBatchBolt.class);

  private static final byte[] TXID = "_txid".getBytes();

  private HTableConnector connector;
  private TupleTableConfig conf;

  private TransactionAttempt attempt;
  private BatchOutputCollector collector;

  // Map of row keys to increments for this batch
  Map<byte[], Increment> counters;

  public HBaseCountersBatchBolt(final TupleTableConfig conf) {
    this.conf = conf;
  }

  /** {@inheritDoc} */
  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map conf, TopologyContext context,
      BatchOutputCollector collector, TransactionAttempt id) {
    this.collector = collector;
    this.attempt = id;
    this.counters = new TreeMap<byte[], Increment>(Bytes.BYTES_COMPARATOR);

    LOG.debug(String.format("Preparing for tx %d (attempt %d)",
        id.getTransactionId(), id.getAttemptId()));
  }

  /** {@inheritDoc} */
  @Override
  public void execute(Tuple tuple) {
    Increment newInc = conf.getIncrementFromTuple(tuple,
        TupleTableConfig.DEFAULT_INCREMENT);

    Increment extInc = counters.get(newInc.getRow());

    if (extInc != null) {
      // Increment already exists for row, add newInc to extInc
      for (Entry<byte[], NavigableMap<byte[], Long>> families : newInc
          .getFamilyMap().entrySet()) {

        for (Entry<byte[], Long> columns : families.getValue().entrySet()) {
          TupleTableConfig.addIncrement(extInc, families.getKey(),
              columns.getKey(), columns.getValue());
        }
      }
      counters.put(newInc.getRow(), extInc);
    } else {
      counters.put(newInc.getRow(), newInc);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void finishBatch() {
    try {
      connector = new HTableConnector(conf);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    LOG.debug("Finishing tx: " + attempt.getTransactionId());
    LOG.debug(String.format(
        "Updating idempotent counters for %d rows in table '%s'",
        counters.size(), conf.getTableName()));

    for (Increment inc : counters.values()) {
      for (Entry<byte[], NavigableMap<byte[], Long>> e : inc.getFamilyMap()
          .entrySet()) {

        for (Entry<byte[], Long> c : e.getValue().entrySet()) {
          // Get counters latest txid from table
          byte[] txidCQ = txidQualifier(c.getKey());
          BigInteger latestTxid = getLatestTxid(inc.getRow(), e.getKey(),
              txidCQ);
          long counter = c.getValue();

          if (latestTxid == null
              || !latestTxid.equals(attempt.getTransactionId())) {
            // txids are different so safe to increment counter
            try {
              counter = connector.getTable().incrementColumnValue(inc.getRow(),
                  e.getKey(), c.getKey(), c.getValue(), conf.isWriteToWAL());
            } catch (IOException ex) {
              throw new RuntimeException(String.format(
                  "Unable to increment counter: %s, %s, %s",
                  Bytes.toString(inc.getRow()), Bytes.toString(e.getKey()),
                  Bytes.toString(c.getKey())), ex);
            }

            putLatestTxid(inc.getRow(), e.getKey(), txidCQ);

            collector.emit(new Values(inc.getRow(), e.getKey(), c.getKey(),
                counter));

            if (LOG.isDebugEnabled()) {
              LOG.debug(String.format(
                  "txids for counter %s, %s, %s are different [%d, %d], incrementing",
                  Bytes.toString(inc.getRow()), Bytes.toString(e.getKey()),
                  Bytes.toString(c.getKey()), latestTxid,
                  attempt.getTransactionId()));
            }

          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug(String.format(
                  "txids for counter %s, %s, %s are the same [%d], skipping",
                  Bytes.toString(inc.getRow()), Bytes.toString(e.getKey()),
                  Bytes.toString(c.getKey()), latestTxid));
            }
          }
        }
      }
    }

  }

  /**
   * Updates the latest txid for the counter
   * 
   * @param row
   *          The row key
   * @param fam
   *          The column family
   * @param qual
   *          The column qualifier of the txid (e.g. the counters qualifier +
   *          "_txid")
   */
  private void putLatestTxid(byte[] row, byte[] fam, byte[] qual) {
    Put txidPut = new Put(row);
    txidPut.add(fam, qual, attempt.getTransactionId().toByteArray());
    try {
      connector.getTable().put(txidPut);
    } catch (IOException e) {
      throw new RuntimeException("Unable to update txid for "
          + txidPut.toString(), e);
    }
  }

  /**
   * Get the latest txid to successfully update the given counter
   * 
   * @param row
   *          The row key
   * @param fam
   *          The column family
   * @param qual
   *          The column qualifier of the txid (e.g. the counters qualifier +
   *          "_txid")
   * @return The latest txid
   */
  private BigInteger getLatestTxid(byte[] row, byte[] fam, byte[] qual) {
    Get getTxid = new Get(row);
    getTxid.addColumn(fam, qual);
    BigInteger latestTxid = null;

    try {
      Result res = connector.getTable().get(getTxid);
      if (!res.isEmpty()) {
        latestTxid = new BigInteger(res.getValue(fam, qual));
      }
    } catch (IOException e) {
      throw new RuntimeException(
          "Unable to get txid for " + getTxid.toString(), e);
    }

    return latestTxid;
  }

  /**
   * Appends "_txid" to the end of the counters column qualifier. Used as an
   * atomic qualifier for getting / setting the latest transaction ID against
   * the counter in HBase
   * 
   * @param cq
   *          The counters column qualifier
   * @return The transaction IDs column qualifier
   */
  private static byte[] txidQualifier(final byte[] cq) {
    byte[] txid = new byte[cq.length + TXID.length];
    System.arraycopy(cq, 0, txid, 0, cq.length);
    System.arraycopy(TXID, 0, txid, cq.length, TXID.length);
    return txid;
  }

  /** {@inheritDoc} */
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("id", "row", "family", "qualifier", "counter"));
  }
}
