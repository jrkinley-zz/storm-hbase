package backtype.storm.contrib.hbase.utils.test;

import junit.framework.Assert;

import org.apache.hadoop.hbase.client.Increment;
import org.junit.Test;

import backtype.storm.contrib.hbase.utils.TupleTableConfig;

public class TestSerialisation {
  private static final byte[] KEY = "http://bit.ly/ZK6t".getBytes();
  private static final byte[] CF = "daily".getBytes();
  private static final byte[] CQ1 = "20120816".getBytes();
  private static final byte[] CQ2 = "20120817".getBytes();

  @Test
  public void testAddIncrement() {
    Increment i = new Increment(KEY);
    i.addColumn(CF, CQ1, 1); // set counter to 1
    i.addColumn(CF, CQ1, 1); // overrides counter, so its still 1

    Assert.assertEquals(1L, (long) i.getFamilyMap().get(CF).get(CQ1));

    TupleTableConfig.addIncrement(i, CF, CQ1, 2L); // increment counter by 2
    TupleTableConfig.addIncrement(i, CF, CQ2, 2L); // increment different
                                                   // qualifier by 2

    Assert.assertEquals(3L, (long) i.getFamilyMap().get(CF).get(CQ1));
    Assert.assertEquals(2L, (long) i.getFamilyMap().get(CF).get(CQ2));
  }
}
