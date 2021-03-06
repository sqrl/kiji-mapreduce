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

package org.kiji.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiConfiguration;

/** Builds a job that runs a MapReduce in Hadoop. */
public class KijiTransformJobBuilder extends MapReduceJobBuilder<KijiTransformJobBuilder> {
  private static final Logger LOG = LoggerFactory.getLogger(KijiTransformJobBuilder.class);

  /** The class of the mapper to run. */
  private Class<? extends KijiMapper> mMapperClass;
  /** The class of the combiner to run. */
  private Class<? extends KijiReducer> mCombinerClass;
  /** The class of the reducer to run. */
  private Class<? extends KijiReducer> mReducerClass;

  /** The mapper instance. */
  private KijiMapper mMapper;
  /** The combiner instance (may be null if no combiner is specified). */
  private KijiReducer mCombiner;
  /** The reducer instance (may be null if no reducer is specified). */
  private KijiReducer mReducer;

  /** The kiji configuration. */
  private KijiConfiguration mKijiConf;
  /** The job input. */
  private MapReduceJobInput mJobInput;

  /** Constructs a builder for jobs that run a MapReduce job to transform data. */
  public KijiTransformJobBuilder() {
    mMapperClass = null;
    mCombinerClass = null;
    mReducerClass = null;

    mMapper = null;
    mCombiner = null;
    mReducer = null;

    mKijiConf = null;
    mJobInput = null;
  }

  /**
   * Sets the base job configuration object to use.
   *
   *  <p>The Configuration instance returned by <code>kijiConf.getConf()</code> will not be
   *  modified; it will be used as the template for the Configuration object created and held
   *  in the underlying Hadoop MapReduce job.</p>
   *
   * @param kijiConf A kiji configuration object
   * @return This builder instance so yuo may chain configuration method calls.
   */
  public KijiTransformJobBuilder withKijiConfiguration(KijiConfiguration kijiConf) {
    mKijiConf = kijiConf;
    return this;
  }

  /**
   * Configures the job with input.
   *
   * @param jobInput The input for the job.
   * @return This builder instance so you may chain configuration method calls.
   */
  public KijiTransformJobBuilder withInput(MapReduceJobInput jobInput) {
    mJobInput = jobInput;
    return this;
  }

  /**
   * Configures the job with a mapper to run in the map phase.
   *
   * @param mapperClass The mapper class.
   * @return This builder instance so you may chain configuration method calls.
   */
  public KijiTransformJobBuilder withMapper(Class<? extends KijiMapper> mapperClass) {
    mMapperClass = mapperClass;
    return this;
  }

  /**
   * Configures the job with a combiner to run (optional).
   *
   * @param combinerClass The combiner class.
   * @return This builder instance so you may chain configuration method calls.
   */
  public KijiTransformJobBuilder withCombiner(
      Class<? extends KijiReducer> combinerClass) {
    mCombinerClass = combinerClass;
    return this;
  }

  /**
   * Configures the job with a reducer to run (optional).
   *
   * @param reducerClass The reducer class.
   * @return This builder instance so you may chain configuration method calls.
   */
  public KijiTransformJobBuilder withReducer(Class<? extends KijiReducer> reducerClass) {
    mReducerClass = reducerClass;
    return this;
  }

  /** {@inheritDoc} */
  @Override
  protected void configureJob(Job job) throws IOException {
    // Check that job input was configured.
    if (null == mJobInput) {
      throw new JobConfigurationException("Must specify job input.");
    }

    // Construct the mapper instance.
    if (null == mMapperClass) {
      throw new JobConfigurationException("Must specify a mapper.");
    }
    mMapper = ReflectionUtils.newInstance(mMapperClass, job.getConfiguration());

    // Construct the combiner instance (if specified).
    if (null != mCombinerClass) {
      mCombiner = ReflectionUtils.newInstance(mCombinerClass, job.getConfiguration());
    }

    // Construct the reducer instance (if specified).
    if (null != mReducerClass) {
      mReducer = ReflectionUtils.newInstance(mReducerClass, job.getConfiguration());
    }

    StringBuilder name = new StringBuilder();
    name.append("Kiji transform: ");
    name.append(mMapperClass.getSimpleName());
    if (null != mReducerClass) {
      name.append(" / ");
      name.append(mReducerClass.getSimpleName());
    }
    job.setJobName(name.toString());

    // Configure the MapReduce job.
    super.configureJob(job);
  }

  /** {@inheritDoc} */
  @Override
  protected Map<String, KeyValueStore<?, ?>> getRequiredStores() {
    Map<String, KeyValueStore<?, ?>> requiredStores = new HashMap<String, KeyValueStore<?, ?>>();
    if (mMapper instanceof KeyValueStoreClient) {
      Map<String, KeyValueStore<?, ?>> mapperStores =
          ((KeyValueStoreClient) mMapper).getRequiredStores();
      if (null != mapperStores) {
        mergeStores(requiredStores, mapperStores);
      }
    }

    if (null != mCombiner && mCombiner instanceof KeyValueStoreClient) {
      Map<String, KeyValueStore<?, ?>> combinerStores =
          ((KeyValueStoreClient) mCombiner).getRequiredStores();
      if (null != combinerStores) {
        mergeStores(requiredStores, combinerStores);
      }
    }

    if (null != mReducer && mReducer instanceof KeyValueStoreClient) {
      Map<String, KeyValueStore<?, ?>> reducerStores =
          ((KeyValueStoreClient) mReducer).getRequiredStores();
      if (null != reducerStores) {
        mergeStores(requiredStores, reducerStores);
      }
    }

    return requiredStores;
  }

  /** {@inheritDoc} */
  @Override
  protected MapReduceJob build(Job job) {
    return new InternalMapReduceJob(job);
  }

  /** {@inheritDoc} */
  @Override
  public Configuration getConf() {
    // Check that the Hadoop configuration was set.
    if (null == mKijiConf) {
      throw new JobConfigurationException("Must specify a configuration");
    }
    return mKijiConf.getConf();
  }

  /** {@inheritDoc} */
  @Override
  protected MapReduceJobInput getJobInput() {
    return mJobInput;
  }

  /** {@inheritDoc} */
  @Override
  protected KijiMapper getMapper() {
    return mMapper;
  }

  /** {@inheritDoc} */
  @Override
  protected KijiReducer getCombiner() {
    return mCombiner;
  }

  /** {@inheritDoc} */
  @Override
  protected KijiReducer getReducer() {
    return mReducer;
  }

  /** {@inheritDoc} */
  @Override
  protected Class<?> getJarClass() {
    return mMapperClass;
  }
}
