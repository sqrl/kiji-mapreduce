/**
 * (c) Copyright 2012 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.mapreduce.context;

import java.io.IOException;

import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import org.kiji.mapreduce.KeyValueStoreReader;
import org.kiji.mapreduce.KeyValueStoreReaderFactory;
import org.kiji.mapreduce.KijiContext;

/** Implements KijiContext. */
public class InternalKijiContext implements KijiContext {
  /** Underlying Hadoop context. */
  private final TaskInputOutputContext mHadoopContext;

  /** Factory for Key/Value stores. */
  private final KeyValueStoreReaderFactory mKeyValueStoreFactory;

  /**
   * Initializes a Kiji context.
   *
   * @param context Underlying Hadoop MapReduce context.
   * @throws IOException on I/O error.
   */
  public InternalKijiContext(TaskInputOutputContext context) throws IOException {
    mHadoopContext = context;
    mKeyValueStoreFactory = new KeyValueStoreReaderFactory(context.getConfiguration());
  }

  /** @return the underlying Hadoop MapReduce context. */
  public TaskInputOutputContext getMapReduceContext() {
    return mHadoopContext;
  }

  /** {@inheritDoc} */
  @Override
  public <K, V> KeyValueStoreReader<K, V> getStore(String storeName) throws IOException,
      InterruptedException {
    return mKeyValueStoreFactory.getStore(storeName);
  }

  /** {@inheritDoc} */
  @Override
  public void incrementCounter(Enum<?> counter) {
    mHadoopContext.getCounter(counter).increment(1);
  }

  /** {@inheritDoc} */
  @Override
  public void incrementCounter(Enum<?> counter, long amount) {
    mHadoopContext.getCounter(counter).increment(amount);
  }

  /** {@inheritDoc} */
  @Override
  public void progress() {
    mHadoopContext.progress();
  }

  /** {@inheritDoc} */
  @Override
  public void setStatus(String msg) throws IOException {
    mHadoopContext.setStatus(msg);
  }

  /** {@inheritDoc} */
  @Override
  public String getStatus() {
    return mHadoopContext.getStatus();
  }

  /** {@inheritDoc} */
  @Override
  public void flush() throws IOException {
    // Do nothing by default.
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    // Do nothing by default.
  }
}
