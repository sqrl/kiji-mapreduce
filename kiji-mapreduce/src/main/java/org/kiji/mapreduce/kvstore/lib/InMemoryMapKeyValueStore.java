package org.kiji.mapreduce.kvstore.lib;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.mapreduce.kvstore.framework.KeyValueStoreConfiguration;

/**
 * KeyValueStore lookup implementation based on a Map&lt;String, String&gt;.
 *
 * <p>This key-value store provides an encapsulated way to pass information to
 * all tasks in a KijiMR job via the key-value store system. This can be
 * useful for things like passing contextual information to Producers that is
 * too ephemeral to store in a KijiTable or file in HDFS (time of day, for
 * example.)</p>
 *
 * <p>Since the entire contents of the map will be stored in the job's
 * Configuration and copied to all tasks and kept in memory it is not
 * recommended that this class be used for very large amounts of data.</p>
 * 
 * <p>To create a InMemoryMapKeyValueStore you should use {@link builder()}.
 * This class has one method,
 * {@link InMemoryMapKeyValueStore.Builder.withMap()} which takes as input the
 * Map&lt;String,String&gt; to be used to back the InMemoryMapKeyValueStore.
 * Note that the contents of this Map is copied when
 * {@link InMemoryMapKeyValueStore.Builder.build()} is called; modifications to
 * the map between the calls to {@link Builder.withMap()} and
 * {@link InMemoryMapKeyValueStore.Builder.build()} will be reflected in the
 * KeyValueStore. Modifications made after the call to build() will not.</p>
 */
@ApiAudience.Public
@ApiStability.Evolving
public final class InMemoryMapKeyValueStore implements KeyValueStore<String, String> {
  private static final String CONF_MAP_KEYS = "map.keys";
  private static final String CONF_MAP_VALUES = "map.values";

  /** The map pulled out of the Configuration object. */
  private Map<String, String> mMap;

  /** true if the user has called open() on this object. */
  private boolean mOpened;

  /**
   * A Builder-pattern class that configures and creates new InMemoryMapKeyValueStore
   * instances. Use this to specify the Map&lt;String, String&gt; for this KeyValueStore
   * and call build() to return a new instance.
   */
  @ApiAudience.Public
  public static final class Builder {
    private Map<String, String> mMap;

    /**
     * Private constructor. Use InMemoryMapKeyValueStore.builder() to get a builder instance.
     */
    private Builder() { }

    /**
     * Sets the map containing the keys and values. Its contents will be copied at the call
     * to build().
     *
     * @param map the map containing the data backing this key value store.
     * @return this builder instance.
     */
    public Builder withMap(Map<String, String> map) {
      mMap = map;
      return this;
    }

    /**
     * Build a new InMemoryMapKeyValueStore instance.
     *
     * @return an initialized KeyValueStore.
     */
    public InMemoryMapKeyValueStore build() {
      if (null == mMap) {
        throw new IllegalArgumentException("Must specify a non-null map.");
      }

      return new InMemoryMapKeyValueStore(this);
    }
  }

  /**
   * Creates a new InMemoryMapKeyValueStore.Builder instance that can be
   * used to 
   * @return
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Reflection-only constructor. Used only for reflection. You should
   * create InMemoryMapKeyValueStore instances by using a builder or
   * factory method. Call
   * {@link InMemoryMapKeyValueStore.get(Map)} to get an instance backed
   * by a map.
   */
  public InMemoryMapKeyValueStore() {
    this(builder());
  }

  /**
   * Constructor that up this KeyValueStore using a builder.
   *
   * @param builder the builder instance to read configuration from.
   */
  private InMemoryMapKeyValueStore(Builder builder) {
    mMap = new HashMap<String, String>(builder.mMap);
  }

  /**
   * Factory method that returns an InMemoryMapKeyValueStore instance.
   * Modifications to the map after this call will not be reflected in the
   * KeyValueStore.
   *
   * @param map the map containing the data for the InMemoryMapKeyValueStore.
   * @return An InMemoryMapKeyValueStore instance.
   */
  public InMemoryMapKeyValueStore get(Map<String, String> map) {
    return builder().withMap(map).build();
  }

  @Override
  public void storeToConf(KeyValueStoreConfiguration conf) throws IOException {
    if (null == mMap) {
      throw new IOException("Required attribute not set: map");
    }
    conf.
    conf.setStrings(name, values)
  }

  @Override
  public void initFromConf(KeyValueStoreConfiguration conf) throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public KeyValueStoreReader<String, String> open() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

}
