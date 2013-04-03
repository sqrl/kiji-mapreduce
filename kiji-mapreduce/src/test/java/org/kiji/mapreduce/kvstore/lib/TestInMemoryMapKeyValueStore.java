package org.kiji.mapreduce.kvstore.lib;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.kiji.mapreduce.KijiMRTestLayouts;
import org.kiji.mapreduce.MapReduceJob;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.mapreduce.kvstore.RequiredStores;
import org.kiji.mapreduce.output.DirectKijiTableMapReduceJobOutput;
import org.kiji.mapreduce.produce.KijiProduceJobBuilder;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.InstanceBuilder;

public class TestInMemoryMapKeyValueStore extends KijiClientTest {
  /**
   * Producer designed to test kvstores. It doesn't actually read any data from the table.
   */
  public static class TestingProducer extends KijiProducer {
    /** Number of entries in our large KV Store. */
    private final static int LARGE_KV_SIZE = 1024;

    @Override
    public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
      // A small map whose value will be checked.
      Map<String, String> smallMap = new HashMap<String, String>();
      smallMap.put("lorem", "ipsum");
      // A larger map to ensure that we can safely encode non-trivial amounts of data.
      Map<String, String> largeMap = new HashMap<String, String>(LARGE_KV_SIZE);
      for (int i = 0; i < LARGE_KV_SIZE; i++) {
        largeMap.put(Integer.toString(i), Integer.toString(i));
      }
      return RequiredStores.with("small", InMemoryMapKeyValueStore.get(smallMap))
          .with("large", InMemoryMapKeyValueStore.get(largeMap));
    }

    @Override
    public KijiDataRequest getDataRequest() {
      // We won't actually use this so it's moot.
      return KijiDataRequest.create("info");
    }

    @Override
    public String getOutputColumn() {
      return "info:first_name";
    }

    @Override
    public void produce(KijiRowData input, ProducerContext context) throws IOException {
      // Ignore the input. Just retrieve our kv stores and confirm their contents.
      try {
        KeyValueStoreReader<String, String> smallStore = context.getStore("small");
        assertEquals("Small store contains incorrect.", "ipsum", smallStore.get("lorem"));
        KeyValueStoreReader<String, String> largeStore = context.getStore("large");
        for (int i = 0; i < LARGE_KV_SIZE; i++) {
          assertEquals("Large store contains incorrect value.",
              Integer.toString(i), largeStore.get(Integer.toString(i)));
        }
        smallStore.close();
        largeStore.close();
      } catch (InterruptedException e) {
        throw new IOException("Interrupted while retrieving stores!", e);
      }
      // Write some data back to the table so we can be sure the producer ran.
      context.put("lorem");
    }
  }

  @Before
  public final void setupTestProducer() throws Exception {
    // Get the test table layouts.
    final KijiTableLayout layout =
        KijiTableLayout.newLayout(KijiMRTestLayouts.getTestLayout());

    // Populate the environment. A small table with one row.
    new InstanceBuilder(getKiji())
        .withTable("test", layout)
            .withRow("Marsellus Wallace")
                .withFamily("info")
                    .withQualifier("first_name").withValue("Marsellus")
                    .withQualifier("last_name").withValue("Wallace")
                    .withQualifier("zip_code").withValue(94110)
        .build();
  }

  @Test
  public void testProducer() throws Exception {
    final KijiURI tableURI = KijiURI.newBuilder(getKiji().getURI()).withTableName("test").build();
    final MapReduceJob job = KijiProduceJobBuilder.create()
        .withConf(getConf())
        .withProducer(TestingProducer.class)
        .withInputTable(tableURI)
        .withOutput(new DirectKijiTableMapReduceJobOutput(tableURI))
        .build();
    // Be sure the job runs successfully and to completion.
    // If the producer finishes, the first_name of the main row will be changed to "lorem".
    assertTrue(job.run());
    final KijiTable table = getKiji().openTable("test");
    final KijiTableReader reader = table.openTableReader();
    String value = reader
        .get(table.getEntityId("Marsellus Wallace"), KijiDataRequest.create("info", "first_name"))
        .getMostRecentValue("info", "first_name").toString();
    assertEquals("Expected producer output not present. Did producer run successfully?",
        "lorem", value);
    reader.close();
    table.release();
  }
}

