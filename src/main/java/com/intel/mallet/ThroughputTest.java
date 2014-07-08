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

import java.util.Arrays;
import java.util.ArrayList;
import java.util.logging.Logger;
import java.util.Date;

public class ThroughputTest {
  private static final Logger logger = Logger.getLogger(ThroughputTest.class.getName());
  private final int throughputTestNumber;
  private final TestPhase testPhase;
  private static int numberOfQueries = -1;
  private static int[] sortedQueryIds = null;
  
  public ThroughputTest(int throughputTestNumber) {
    assert (throughputTestNumber >= 1 && throughputTestNumber <= 2);
    this.throughputTestNumber = throughputTestNumber;
    if (throughputTestNumber == 1) {
      testPhase = TestPhase.THROUGHPUT_TEST_1;
    } else {
      testPhase = TestPhase.THROUGHPUT_TEST_2;
    }
  }

  public static int getNumberOfQueries() {
    return numberOfQueries;
  }
  
  public static int[] getSortedQueryIds() {
   return sortedQueryIds;
  }

  // Per the TPC-DS spec, concurrent execution of refresh data sets and queries is not required.
  // It is chosen here that refresh data sets are to be executed alone as HIVE lacks ACID support.
  public void run() throws MalletException {
    String context = "Throughput Test " + throughputTestNumber;
    logger.info(context + " starts.");

    Metrics metrics = Metrics.getMetrics();
    metrics.addPhaseMetric(testPhase, true, new Date());

    int numberOfStreams = Conf.getConf().getNumberOfStreams();
    logger.info(context + " has " + numberOfStreams + " streams.");

    // Invoke dsqgen tool to generate queries for all streams.
    String[] sqlFiles = TpcdsTool.generateStreamSqlFile(numberOfStreams);

    // Invoke dsdgen tool to generate refresh data sets.
    TpcdsTool.generateRefreshDataSets();
    
    Coordinator coordinator = null;
    
    MalletThread[] threads = new MalletThread[numberOfStreams + 1];    

    // create Query Stream threads
    for (int i = 0; i < numberOfStreams; i++) {
      QueryStream stream = new QueryStream(testPhase, i, sqlFiles[i]);;
      threads[i] = stream;
      
      // Make sure all streams have the same number of queries
      if (numberOfQueries == -1) {
        numberOfQueries = stream.getNumberOfQueries();
        if (numberOfQueries <= 3) {
          // The number of queries between two consecutive refresh sets is
          // determined by the following fomula:
          //    2*(numberOfQueries - 3).
          // for example, if numberOfQueries = 99, then the number of queries
          // between two consecutive refresh sets is 2*(99-3)=192.
          
          throw new MalletException("Too few queries in SQL file: " + sqlFiles[0]);
        }
        int[] queryIds = stream.getQueryIds();
        sortedQueryIds = Arrays.copyOf(queryIds, queryIds.length);
        Arrays.sort(sortedQueryIds);
      } else {
        if (numberOfQueries != stream.getNumberOfQueries()) {
          throw new MalletException("Inconsistent SQL files with different number of queries.");
        }
        int[] queryIds = stream.getQueryIds();
        int[] tempQueryIds = Arrays.copyOf(queryIds, queryIds.length);
        Arrays.sort(tempQueryIds);
        if (!Arrays.equals(sortedQueryIds, tempQueryIds)) {
          throw new MalletException("Inconsistent SQL files with different query IDs.");
        }
      }
      
      if (i == 0) {
       // Create Coordinator object to coordinate among query streams and refresh group.
        coordinator = new Coordinator(numberOfStreams, numberOfQueries);
        stream.setCoordinator(coordinator);
      } else {
        stream.setCoordinator(coordinator);
      } 
    }
    
    // create Refresh Group thread
    RefreshGroup refreshGroup = new RefreshGroup(testPhase, coordinator);
    threads[numberOfStreams] = refreshGroup;

    // Start all threads
    for (int i = 0; i <= numberOfStreams; i++) {
      threads[i].start();
    }
    
    // Wait for all stream threads and Refresh Group thread to finish
    int endedThreadCount = 0;
    while (endedThreadCount < numberOfStreams + 1) {
      // Sleep for a while before checking status of working threads
      try {
        Thread.currentThread().sleep(1000);
      } catch (InterruptedException e) {
      }
      
      for (int i = 0; i <= numberOfStreams; i++) {
        MalletThread thread = threads[i];
        if (thread != null && !thread.isAlive()) {
          threads[i] = null;
          endedThreadCount++;
          if (thread.getExitCause() != null) {
            // Due to the blocking nature of JDBC and HIVE server implementation,
            // we very likely can't gracefully stop all coordinating threads,
            // so throw exception to abort the program.
            throw new MalletException(thread.getName() + " aborted.", thread.getExitCause());
          }
        }
      }
    }

    metrics.addPhaseMetric(testPhase, false, new Date());

    logger.info(context + " ends.");
  }
}
