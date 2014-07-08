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

// Class for Reporter to display the metrics.
public class Reporter {
  private static final Logger logger = Logger.getLogger(Metrics.class.getName());
  private static final double SCALE = 1000;
  private static final Metrics metrics = Metrics.getMetrics();

  public Reporter() {
  }
  
  public static void report() {
    Conf conf = Conf.getConf();

    if (Conf.getConf().isPowerTestOnly()) {
      String basicInfo = "------------------- Mallet Benchmark Report --------------------" + "\n" +
        "Database Load Elapsed Time:        " + runTimeFormat(metrics.getLoadTestTime()) + "\n" +
        "Power Test Elapsed Time:           " + runTimeFormat(metrics.getPowerTestTime()) + "\n"; 
      System.out.println(basicInfo);
      reportPowerTestTiming();
      return;
    }
    
    int numberOfStreams = conf.getNumberOfStreams();
    int numberOfQueries = ThroughputTest.getNumberOfQueries();

    String basicInfo = "------------------- Mallet Benchmark Report --------------------" + "\n" +
      "Number of Query Streams:           " + numberOfStreams + "\n" + 
      "Number of queries in Query Stream: " + numberOfQueries + "\n\n" +
      "Database Load Elapsed Time:        " + runTimeFormat(metrics.getLoadTestTime()) + "\n" +
      "Power Test Elapsed Time:           " + runTimeFormat(metrics.getPowerTestTime()) + "\n" +
      "Throughput Run 1 Elapsed Time:     " + runTimeFormat(metrics.getThroughputTest1Time()) + "\n" +
      "Query Run 1 Elapsed Time:          " + runTimeFormat(metrics.getQueryRun1Time()) + "\n" +
      "Refresh Run 1 Elapsed Time:        " + runTimeFormat(metrics.getRefreshRun1Time()) + "\n" +
      "Throughput Run 2 Elapsed Time:     " + runTimeFormat(metrics.getThroughputTest2Time()) + "\n" +
      "Query Run 2 Elapsed Time:          " + runTimeFormat(metrics.getQueryRun2Time()) + "\n" +
      "Refresh Run 2 Elapsed Time:        " + runTimeFormat(metrics.getRefreshRun2Time()) + "\n" +
      "----------------------------------------------------------------";
    System.out.println(basicInfo);
                  
    System.out.println("Performance Metric = " + metrics.getPerformanceMetric() + " QphM@" + conf.getScale() + "GB");
    System.out.println("----------------------------------------------------------------");

    reportQueryRunTiming(1);
    reportQueryRunTiming(2);
  }

  private static void reportPowerTestTiming() {
    int[] queryIds = PowerTest.getQueryIds();
    for (int i = 0; i < queryIds.length; i++) {
      System.out.println("Query " + queryIds[i] + " time: " + runTimeFormat(metrics.getPowerTestQueryTime(queryIds[i])));
    }
  }
  
  private static void reportQueryRunTiming(int queryRunNumber) {
    assert(queryRunNumber == 1 || queryRunNumber == 2);
    
    int numberOfQueries = ThroughputTest.getNumberOfQueries();
    int[] queryIds = ThroughputTest.getSortedQueryIds();
    
    long[][] queryRunStatistics;
    
    if (queryRunNumber == 1) {
      queryRunStatistics = metrics.getQueryRun1Statistics();
    } else {
      queryRunStatistics = metrics.getQueryRun2Statistics();
    }

    String runQueryInfo = "---------- Query Run " + queryRunNumber + " Timing Intervals (in seconds) -----------\n" +
      "Query\tMinimum\t\tMedian\t\tMaximum\n";

    for (int i = 0; i < numberOfQueries; i++) {
      long[] queryStatistics = queryRunStatistics[i];
      runQueryInfo += queryIds[i] + "\t" + queryStatistics[0] / SCALE+ "\t\t" + queryStatistics[1] / SCALE + "\t\t" + queryStatistics[2] / SCALE + "\n";
    }

    System.out.print(runQueryInfo);    
  }
  
  private static String runTimeFormat(long milliSeconds) {
    long seconds = milliSeconds / 1000;
    long hours = seconds / 3600;
    long minutes = (seconds % 3600) / 60;
    seconds = (seconds % 3600) % 60;
    return hours + "h:" + minutes + "m:" + seconds + "s";
  }
}
