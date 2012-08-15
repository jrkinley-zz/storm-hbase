package backtype.storm.contrib.hbase.utils;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import backtype.storm.tuple.Tuple;

/**
 * Configuration for Storm {@link Tuple} to HBase table mappings.
 * <p>
 * Defines the tuple fields used to set the tables rowKey, columnFamily, and
 * timestamp. The remainder of the tuple fields are used to set the
 * columnQualifiers and values.
 */
public class TupleTableConfig {
  private String tableName;
  private String tupleRowKeyField;
  private String tupleFamilyField;
  private String tupleTimestampField;
  private boolean batch = true;

  /**
   * Initialize configuration
   * 
   * @param table
   *          The HBase table name
   * @param rowKeyField
   *          The {@link Tuple} field used to set the rowKey
   * @param familyField
   *          The {@link Tuple} field used to set the columnFamily
   */
  public TupleTableConfig(final String table, final String rowKeyField,
      final String familyField) {
    this.tableName = table;
    this.tupleRowKeyField = rowKeyField;
    this.tupleFamilyField = familyField;
    this.tupleTimestampField = "";
  }

  /**
   * Initialize configuration
   * 
   * @param table
   *          The HBase table name
   * @param rowKeyField
   *          The {@link Tuple} field used to set the rowKey
   * @param familyField
   *          The {@link Tuple} field used to set the columnFamily
   * @param timestampField
   *          The {@link Tuple} field used to set the timestamp
   */
  public TupleTableConfig(final String table, final String rowKeyField,
      final String familyField, final String timestampField) {
    this.tableName = table;
    this.tupleRowKeyField = rowKeyField;
    this.tupleFamilyField = familyField;
    this.tupleTimestampField = timestampField;
  }

  /**
   * Creates a HTable {@link Put} from a Storm {@link Tuple}
   * 
   * @param tuple
   *          The {@link Tuple}
   * @return {@link Put}
   */
  public Put getPutFromTuple(final Tuple tuple) {
    byte[] rowKey = Bytes.toBytes(tuple.getStringByField(tupleRowKeyField));
    byte[] cf = Bytes.toBytes(tuple.getStringByField(tupleFamilyField));
    long ts = 0;

    if (!tupleTimestampField.equals("")) {
      ts = tuple.getLongByField(tupleTimestampField);
    }

    Put p = new Put(rowKey);

    for (String field : tuple.getFields()) {
      if (includeField(field)) {
        byte[] cq = Bytes.toBytes(field);
        byte[] val = Bytes.toBytes(tuple.getStringByField(field));

        if (ts > 0) {
          p.add(cf, cq, ts, val);
        } else {
          p.add(cf, cq, val);
        }
      }
    }

    return p;
  }

  /**
   * Checks whether to include field as a table column
   * <p>
   * The configured rowKey, columnFamily, and timestamp fields are ignored
   * 
   * @param field
   * @return boolean
   */
  private boolean includeField(String field) {
    if (field.equalsIgnoreCase(tupleRowKeyField)
        || field.equalsIgnoreCase(tupleFamilyField)) {
      return false;
    }

    if (!tupleTimestampField.equals("")
        && field.equalsIgnoreCase(tupleTimestampField)) {
      return false;
    }

    return true;
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
