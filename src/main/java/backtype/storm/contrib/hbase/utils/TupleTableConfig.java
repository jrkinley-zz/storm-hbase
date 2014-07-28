package backtype.storm.contrib.hbase.utils;

import java.io.Serializable;
import java.util.*;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import backtype.storm.tuple.Tuple;

import javax.annotation.Nullable;

/**
 * Configuration for Storm {@link Tuple} to HBase serialization.
 */
@SuppressWarnings("serial")
public class TupleTableConfig implements Serializable {

  public static final long DEFAULT_INCREMENT = 1L;

  private String tableName;
  protected String tupleRowKeyField;
  protected String tupleTimestampField;
  protected Map<String, Set<String>> columnFamilies;
  private boolean batch = true;
  protected boolean writeToWAL = true;
  private long writeBufferSize = 0L;

  /**
   * Initialize configuration
   * @param table The HBase table name
   * @param rowKeyField The {@link Tuple} field used to set the rowKey
   */
  public TupleTableConfig(final String table, final String rowKeyField) {
    this.tableName = table;
    this.tupleRowKeyField = rowKeyField;
    this.tupleTimestampField = "";
    this.columnFamilies = new HashMap<String, Set<String>>();
  }

  /**
   * Initialize configuration
   * @param table The HBase table name
   * @param rowKeyField The {@link Tuple} field used to set the rowKey
   * @param timestampField The {@link Tuple} field used to set the timestamp
   */
  public TupleTableConfig(final String table, final String rowKeyField, final String timestampField) {
    this.tableName = table;
    this.tupleRowKeyField = rowKeyField;
    this.tupleTimestampField = timestampField;
    this.columnFamilies = new HashMap<String, Set<String>>();
  }

  /**
   * Add column family and column qualifier to be extracted from tuple
   * @param columnFamily The column family name
   * @param columnQualifier The column qualifier name
   */
  public void addColumn(final String columnFamily, final String columnQualifier) {
    Set<String> columns = this.columnFamilies.get(columnFamily);

    if (columns == null) {
      columns = new HashSet<String>();
    }
    columns.add(columnQualifier);

    this.columnFamilies.put(columnFamily, columns);
  }

  /**
   * Creates a HBase {@link Put} from a Storm {@link Tuple}
   * @param tuple The {@link Tuple}
   * @return {@link Put}
   */
  public Put getPutFromTuple(final Tuple tuple) {
    byte[] rowKey = Bytes.toBytes(tuple.getStringByField(tupleRowKeyField));

    long ts = 0;
    if (!tupleTimestampField.equals("")) {
      ts = tuple.getLongByField(tupleTimestampField);
    }

    Put p = new Put(rowKey);
    p.setWriteToWAL(writeToWAL);

    if (columnFamilies.size() > 0) {
      for (String cf : columnFamilies.keySet()) {
        byte[] cfBytes = Bytes.toBytes(cf);
        for (String cq : columnFamilies.get(cf)) {
          byte[] cqBytes = Bytes.toBytes(cq);
          byte[] val = Bytes.toBytes(tuple.getStringByField(cq));

          if (ts > 0) {
            p.add(cfBytes, cqBytes, ts, val);
          } else {
            p.add(cfBytes, cqBytes, val);
          }
        }
      }
    }

    return p;
  }

  /**
   * Creates a HBase {@link Increment} from a Storm {@link Tuple}
   * @param tuple The {@link Tuple}
   * @param increment The amount to increment the counter by
   * @return {@link Increment}
   */
  public Increment getIncrementFromTuple(final Tuple tuple, final long increment) {
    byte[] rowKey = Bytes.toBytes(tuple.getStringByField(tupleRowKeyField));

    Increment inc = new Increment(rowKey);
    inc.setWriteToWAL(writeToWAL);

    if (columnFamilies.size() > 0) {
      for (String cf : columnFamilies.keySet()) {
        byte[] cfBytes = Bytes.toBytes(cf);
        for (String cq : columnFamilies.get(cf)) {
          byte[] val;
          try {
            val = Bytes.toBytes(tuple.getStringByField(cq));
          } catch (IllegalArgumentException ex) {
            // if cq isn't a tuple field, use cq for counter instead of tuple
            // value
            val = Bytes.toBytes(cq);
          }
          inc.addColumn(cfBytes, val, increment);
        }
      }
    }

    return inc;
  }

  /**
   * Increment the counter for the given family and column by the specified amount
   * <p>
   * If the family and column already exist in the Increment the counter value is incremented by the
   * specified amount rather than overridden, as it is in HBase's
   * {@link Increment#addColumn(byte[], byte[], long)} method
   * @param inc The {@link Increment} to update
   * @param family The column family
   * @param qualifier The column qualifier
   * @param amount The amount to increment the counter by
   */
  public static void addIncrement(Increment inc, final byte[] family, final byte[] qualifier,
                                  final Long amount) {

    List<Cell> origCells = inc.getFamilyCellMap().get(family);
    if (origCells == null) {
      origCells = new ArrayList<Cell>();
    }

    // get a reversed view of the list to find the last matching cell
    List<Cell> cells = Lists.reverse(origCells);

    int cellIndex = Iterables.indexOf(cells, new Predicate<Cell>() {
      @Override
      public boolean apply(@Nullable Cell cell) {
        if (CellUtil.matchingQualifier(cell, qualifier)) {
          return true;
        }
        return false;
      }
    });

    if (cellIndex < 0) {
      inc.addColumn(family, qualifier, amount);
    } else {
      Cell cell = cells.get(cellIndex);
      long counter = Bytes.toLong(cell.getValueArray(),
              cell.getValueOffset(), cell.getValueLength());
      //replace the last matching cell
      cell = CellUtil.createCell(inc.getRow(), family, qualifier, cell.getTimestamp(), cell.getTypeByte(),
              Bytes.toBytes(counter + amount));
      cells.set(cellIndex, cell);
      inc.getFamilyCellMap().put(inc.getRow(), origCells);
    }
  }

  /**
   * @return the tableName
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * @return Whether batch mode is enabled
   */
  public boolean isBatch() {
    return batch;
  }

  /**
   * @param batch Whether to enable HBase's client-side write buffer.
   *          <p>
   *          When enabled your bolt will store put operations locally until the write buffer is
   *          full, so they can be sent to HBase in a single RPC call. When disabled each put
   *          operation is effectively an RPC and is sent straight to HBase. As your bolt can
   *          process thousands of values per second it is recommended that the write buffer is
   *          enabled.
   *          <p>
   *          Enabled by default
   */
  public void setBatch(boolean batch) {
    this.batch = batch;
  }

  /**
   * @param writeToWAL Sets whether to write to HBase's edit log.
   *          <p>
   *          Setting to false will mean fewer operations to perform when writing to HBase and hence
   *          better performance, but changes that haven't been flushed to a store file will be lost
   *          in the event of HBase failure
   *          <p>
   *          Enabled by default
   */
  public void setWriteToWAL(boolean writeToWAL) {
    this.writeToWAL = writeToWAL;
  }

  /**
   * @return True if write to HBase's edit log (WAL), false if not
   */
  public boolean isWriteToWAL() {
    return writeToWAL;
  }

  /**
   * @param writeBufferSize Overrides the client-side write buffer size.
   *          <p>
   *          By default the write buffer size is 2 MB (2097152 bytes). If you are storing larger
   *          data, you may want to consider increasing this value to allow your bolt to efficiently
   *          group together a larger number of records per RPC
   *          <p>
   *          Overrides the write buffer size you have set in your hbase-site.xml e.g.
   *          <code>hbase.client.write.buffer</code>
   */
  public void setWriteBufferSize(long writeBufferSize) {
    this.writeBufferSize = writeBufferSize;
  }

  /**
   * @return the writeBufferSize
   */
  public long getWriteBufferSize() {
    return writeBufferSize;
  }

  /**
   * @return A Set of configured column families
   */
  public Set<String> getColumnFamilies() {
    return this.columnFamilies.keySet();
  }

  /**
   * @return the tupleRowKeyField
   */
  public String getTupleRowKeyField() {
    return tupleRowKeyField;
  }
}
