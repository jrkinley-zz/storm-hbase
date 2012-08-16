package backtype.storm.contrib.hbase.utils;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;

/**
 * HTable connector for storm bolt
 */
@SuppressWarnings("serial")
public class HTableConnector implements Serializable {
  private static final Logger LOG = Logger.getLogger(HTableConnector.class);

  private Configuration conf;
  protected HTable table;
  private String tableName;
  private boolean isBatch;

  /**
   * Initialize HTable connection
   * 
   * @param tableName
   *          The HBase table name
   * @param batch
   *          Whether to enable HBase's client-side write buffer. When enabled
   *          your bolt will store put operations locally until the write buffer
   *          is full, so they can be sent to HBase in a single RPC call. When
   *          disabled each put operation is effectively an RPC and is sent
   *          straight to HBase. As your bolt can process thousands of values
   *          per second it is recommended that the write buffer is enabled.
   * @throws IOException
   */
  public HTableConnector(String tableName, boolean batch) throws IOException {
    this.tableName = tableName;
    this.conf = HBaseConfiguration.create();
    this.isBatch = batch;

    LOG.info(String.format("Initializing connection to HBase table %s at %s",
        tableName, this.conf.get("hbase.rootdir")));

    try {
      this.table = new HTable(this.conf, tableName);
    } catch (IOException ex) {
      throw new IOException("Unable to establish connection to HBase table "
          + tableName, ex);
    }

    if (batch) {
      // Enable client-side write buffer
      this.table.setAutoFlush(false, true);
      LOG.info("Batch mode enabled");
    }
  }

  /**
   * Put some data in the table
   * <p>
   * If batch mode is enabled, the update will be buffered until the buffer is
   * full or {@link #flush()} is called
   * 
   * @param put
   * @throws IOException
   */
  public void put(Put put) throws IOException {
    this.table.put(put);
  }

  /**
   * Increments one or more columns within a single row
   * 
   * @param increment
   * @throws IOException
   */
  public void increment(Increment increment) throws IOException {
    this.table.increment(increment);
  }

  /**
   * Explicitly flush the write buffer
   * <p>
   * Puts are automatically flushed when not in batch mode
   */
  public void flush() {
    try {
      this.table.flushCommits();
    } catch (IOException ex) {
      LOG.error("Unable to flush write buffer on HBase table " + tableName, ex);
    }
  }

  /**
   * Close the table
   */
  public void close() {
    try {
      this.table.close();
    } catch (IOException ex) {
      LOG.error("Unable to close connection to HBase table " + tableName, ex);
    }
  }

  /**
   * Set the client-side write buffer size for this HTable.
   * <p>
   * By default the write buffer size is 2 MB. If you are storing larger data,
   * you may want to consider increasing this value to allow your bolt to
   * efficiently group together a larger number of records per RPC
   * <p>
   * Calling this method overrides the write buffer size you have set in your
   * hbase-site.xml e.g. <code>hbase.client.write.buffer</code>
   * 
   * @param writeBufferSize
   *          The client-side write buffer size in bytes
   */
  public void setWriteBufferSize(long writeBufferSize) {
    if (this.table != null) {
      try {
        this.table.setWriteBufferSize(writeBufferSize);
      } catch (IOException ex) {
        LOG.error(
            "Unable to set client-side write buffer size for HBase table "
                + tableName, ex);
      }
    }
  }

  /**
   * @return the isBatch
   */
  public boolean isBatch() {
    return isBatch;
  }
}
