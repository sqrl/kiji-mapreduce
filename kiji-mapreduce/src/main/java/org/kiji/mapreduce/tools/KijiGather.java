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

package org.kiji.mapreduce.tools;

import java.io.IOException;

import org.kiji.common.flags.Flag;
import org.kiji.mapreduce.KijiGatherJobBuilder;
import org.kiji.mapreduce.KijiGatherer;
import org.kiji.mapreduce.KijiReducer;
import org.kiji.schema.tools.KijiToolLauncher;

/** Command-line tool for running a KijiGatherer. */
public class KijiGather extends KijiJobTool<KijiGatherJobBuilder> {
  @Flag(name="gatherer", usage="Fully-qualified class name of the gatherer to run")
  private String mGathererName = "";

  @Flag(name="combiner", usage="Fully-qualifier class name of the combiner to use (optional)")
  private String mCombinerName = "";

  @Flag(name="reducer", usage="Fully-qualified class name of the reducer to run")
  private String mReducerName = "";

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "gather";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Run a KijiGatherer over a table";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "MapReduce";
  }

  @Override
  protected void validateFlags() throws Exception {
    super.validateFlags();
    if (mGathererName.isEmpty()) {
      throw new RequiredFlagException("gatherer");
    }
  }

  @Override
  protected KijiGatherJobBuilder createJobBuilder() {
    return new KijiGatherJobBuilder();
  }

  /**
   * Verifies that theClass can be cast to an instance of theInterface.
   * Throws ClassCastException if theClass cannot be cast as theInterface.
   *
   * @param theClass a class to check if it can be cast to an interface.
   * @param theInterface the target superclass or interface of theClass.
   */
  private void checkSubclass(Class<?> theClass, Class<?> theInterface) {
    if (!theInterface.isAssignableFrom(theClass)) {
      throw new ClassCastException("Error: Specified class " + theClass.getName()
          + " must extend " + theInterface.getName());
    }
  }

  @Override
  protected void configure(KijiGatherJobBuilder jobBuilder)
      throws ClassNotFoundException, IOException, JobIOSpecParseException {
    super.configure(jobBuilder);
    Class<?> gathererClass = Class.forName(mGathererName);
    checkSubclass(gathererClass, KijiGatherer.class);
    jobBuilder.withGatherer(gathererClass.asSubclass(KijiGatherer.class));

    if (!mCombinerName.isEmpty()) {
      Class<?> combinerClass = Class.forName(mCombinerName);
      checkSubclass(combinerClass, KijiReducer.class);
      jobBuilder.withCombiner(combinerClass.asSubclass(KijiReducer.class));
    }
    if (!mReducerName.isEmpty()) {
      Class<?> reducerClass = Class.forName(mReducerName);
      checkSubclass(reducerClass, KijiReducer.class);
      jobBuilder.withReducer(reducerClass.asSubclass(KijiReducer.class));
    }
    MapReduceJobOutputFactory outputFactory = new MapReduceJobOutputFactory();
    jobBuilder.withOutput(outputFactory.createFromOutputSpec(mOutputSpec));
  }

  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(new KijiToolLauncher().run(new KijiGather(), args));
  }
}
