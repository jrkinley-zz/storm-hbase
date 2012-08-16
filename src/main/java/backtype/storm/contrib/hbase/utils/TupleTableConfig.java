package backtype.storm.contrib.hbase.utils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import backtype.storm.tuple.Tuple;

/**
 * Configuration for Storm {@link Tuple} to HBase table mappings.
 */
@SuppressWarnings("serial")
public class TupleTableConfig implements Serializable {
  private String tableName;
  private String tupleRowKeyField;
  private String tupleTimestampField;
  private Map<String, Set<String>> columnFamilies;
  private boolean batch = true;

  /**
   * Initialize configuration
   * 
   * @param table
   *          The HBase table name
   * @param rowKeyField
   *          The {@link Tuple} field used to set the rowKey
   */
  public TupleTableConfig(final String table, final String rowKeyField) {
    this.tableName = table;
    this.tupleRowKeyField = rowKeyField;
    this.tupleTimestampField = "";
    this.columnFamilies = new HashMap<String, Set<String>>();
  }

  /**
   * Initialize configuration
   * 
   * @param table
   *          The HBase table name
   * @param rowKeyField
   *          The {@link Tuple} field used to set the rowKey
   * @param timestampField
   *          The {@link Tuple} field used to set the timestamp
   */
  public TupleTableConfig(final String table, final String rowKeyField,
      final String timestampField) {
    this.tableName = table;
    this.tupleRowKeyField = rowKeyField;
    this.tupleTimestampField = timestampField;
    this.columnFamilies = new HashMap<String, Set<String>>();
  }

  /**
   * Add column family and column qualifier to be extracted from tuple
   * 
   * @param columnFamily
   *          The column family name
   * @param columnQualifier
   *          The column qualifier name
   */
  public void addColumn(final String columnFamily, String columnQualifier) {
    Set<String> columns = this.columnFamilies.get(columnFamily);

    if (columns == null) {
      columns = new HashSet<String>();
    }
    columns.add(columnQualifier);

    this.columnFamilies.put(columnFamily, columns);
  }

  /**
   * Creates a HBase {@link Put} from a Storm {@link Tuple}
   * 
   * @param tuple
   *          The {@link Tuple}
   * @return {@link Put}
   */
  public Put getPutFromTuple(final Tuple tuple) {
    byte[] rowKey = Bytes.toBytes(tuple.getStringByField(tupleRowKeyField));

    long ts = 0;
    if (!tupleTimestampField.equals("")) {
      ts = tuple.getLongByField(tupleTimestampField);
    }

    Put p = new Put(rowKey);

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
   * 
   * @param tuple
   *          The {@link Tuple}
   * @param increment
   *          The amount to increment the counter by
   * @return {@link Increment}
   */
  public Increment getIncrementFromTuple(final Tuple tuple, final long increment) {
    byte[] rowKey = Bytes.toBytes(tuple.getStringByField(tupleRowKeyField));

    Increment inc = new Increment(rowKey);

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
   * @param batch
   *          Enable or disable batch mode
   */
  public void setBatch(boolean batch) {
    this.batch = batch;
  }
}
