package backtype.storm.contrib.hbase.utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import storm.trident.state.JSONNonTransactionalSerializer;
import storm.trident.state.JSONOpaqueSerializer;
import storm.trident.state.JSONTransactionalSerializer;
import storm.trident.state.Serializer;
import storm.trident.state.StateType;
import storm.trident.tuple.TridentTuple;

import com.esotericsoftware.minlog.Log;

/**
 * Configuration for Storm Trident state persistence in HBase
 * @param <T>
 */
@SuppressWarnings("serial")
public class TridentConfig<T> extends TupleTableConfig {
  @SuppressWarnings("rawtypes")
  public static final Map<StateType, Serializer> DEFAULT_SERIALZERS =
      new HashMap<StateType, Serializer>() {
        {
          put(StateType.NON_TRANSACTIONAL, new JSONNonTransactionalSerializer());
          put(StateType.TRANSACTIONAL, new JSONTransactionalSerializer());
          put(StateType.OPAQUE, new JSONOpaqueSerializer());
        }
      };

  private int stateCacheSize = 1000;
  private Serializer<T> stateSerializer;

  public TridentConfig(String table, String rowKeyField) {
    super(table, rowKeyField);
  }

  public TridentConfig(final String table, final String rowKeyField, final String timestampField) {
    super(table, rowKeyField, timestampField);
  }

  /**
   * Creates a HBase {@link Put} from a Storm {@link TridentTuple}
   * @param tuple The {@link TridentTuple}
   * @return {@link Put}
   */
  public Put getPutFromTridentTuple(final TridentTuple tuple) {
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
   * Creates a HBase {@link Get} from a Storm {@link TridentTuple}
   * @param tuple The {@link TridentTuple}
   * @return {@link Get}
   */
  public Get getGetFromTridentTuple(final TridentTuple tuple) {
    byte[] rowKey = Bytes.toBytes(tuple.getStringByField(tupleRowKeyField));

    long ts = 0;
    if (!tupleTimestampField.equals("")) {
      ts = tuple.getLongByField(tupleTimestampField);
    }

    Get g = new Get(rowKey);

    if (columnFamilies.size() > 0) {
      for (String cf : columnFamilies.keySet()) {
        byte[] cfBytes = Bytes.toBytes(cf);
        for (String cq : columnFamilies.get(cf)) {
          byte[] cqBytes = Bytes.toBytes(cq);
          g.addColumn(cfBytes, cqBytes);
          try {
            g.setMaxVersions(1);
          } catch (IOException e) {
            Log.error("Invalid number of versions", e);
          }
          if (ts > 0) {
            g.setTimeStamp(ts);
          }
        }
      }
    }

    return g;
  }

  /**
   * @return The size of the least-recently-used (LRU) cache. <b>Default is 1000
   */
  public int getStateCacheSize() {
    return stateCacheSize;
  }

  /**
   * @param stateCacheSize Sets the size of the least-recently-used (LRU) cache. <b>Default is 1000
   */
  public void setStateCacheSize(int stateCacheSize) {
    this.stateCacheSize = stateCacheSize;
  }

  /**
   * @return The {@link Serializer} used for persisting Trident state to HBase
   */
  public Serializer<T> getStateSerializer() {
    return stateSerializer;
  }

  /**
   * @param stateSerializer Set the {@link Serializer} to use for persisting Trident state to HBase
   */
  public void setStateSerializer(Serializer<T> stateSerializer) {
    this.stateSerializer = stateSerializer;
  }
}
