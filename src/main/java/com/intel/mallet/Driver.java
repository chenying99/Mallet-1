/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.intel.mallet;

import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Driver of Mallet benchmark on HIVE.
 *
 */
public class Driver 
{
  private static final Logger logger = Logger.getLogger(Driver.class.getName());
  
  public static void main(String[] args) throws Exception
  {
    MalletThread.setUncaughtExceptionHandler();
    
    logger.info("Mallet Big Data Benchmark.");
    Thread.currentThread().setName("Mallet Driver");
    
    Conf.getConf().parseCommandLine(args);
    
    // Loader Test.
    LoadTest loadTest = new LoadTest();
    try {
      loadTest.run();
    } catch (MalletException e) {
      logger.log(Level.SEVERE, "Load Test failed. Program aborted.", e);
      System.exit(2);
    }
       
    // Power Test.
    PowerTest powerTest = new PowerTest();
    try {
      powerTest.run();
    } catch (MalletException e) {
      logger.log(Level.SEVERE, "Power Test failed. Program aborted.", e);
      System.exit(3);
    }
    
    if (!Conf.getConf().isPowerTestOnly()) {
      // Throughput Test 1.
      // TODO: get number of streams from Conf.
      ThroughputTest throughputTest1 = new ThroughputTest(1);
      try {
        throughputTest1.run();
      } catch (MalletException e) {
        logger.log(Level.SEVERE, "Throughput Test 1 failed. Program aborted.", e);
        System.exit(4);
      }
      
      // Throughput Test 2.
      ThroughputTest throughputTest2 = new ThroughputTest(2);
      try {
        throughputTest2.run();
      } catch (MalletException e) {
        logger.log(Level.SEVERE, "Throughput Test 2 failed. Program aborted.", e);
        System.exit(5);
      }
    }
    
    // Generate report from Metrics.
    Reporter.report();
    System.exit(0);
  }
}
