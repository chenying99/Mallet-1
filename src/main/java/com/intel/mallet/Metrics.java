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
import java.util.logging.Logger;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;

// Class for Metrics collection.
public class Metrics {
  private static final Logger logger = Logger.getLogger(Metrics.class.getName());

  private HashMap<String, Date> dataMetricsMap = new HashMap<String, Date>();
  private static Metrics metrics = new Metrics();
  private Conf conf = Conf.getConf();

  private Metrics() {
  }

  public static Metrics getMetrics() {
    return metrics;
  }
  
  private String getPhaseMetricName(TestPhase testPhase, boolean isStartTime) {
    String metricName = testPhase.toString() + "_";
    if (isStartTime) {
      metricName += "start";
    } else {
      metricName += "end";
    }

    return metricName;
  }
  
  public void addPhaseMetric(TestPhase testPhase, boolean isStartTime, Date timestamp) {
    dataMetricsMap.put(getPhaseMetricName(testPhase, isStartTime), timestamp);
  }

  private String getQueryMetricName(TestPhase testPhase, int streamId, int queryId, boolean isStartTime) {
    String metricName = testPhase.toString() + "_query_" + streamId + "_" + queryId + "_";
    if (isStartTime) {
      metricName += "start";
    } else {
      metricName += "end";
    }

    return metricName;
  }
  
  // synchronized method for parallel query running
  public synchronized void addQueryMetric(TestPhase testPhase, int streamId, int queryId, boolean isStartTime, Date timestamp) {
    dataMetricsMap.put(getQueryMetricName(testPhase, streamId, queryId, isStartTime), timestamp);
  }

  private String getRefreshMetricName(TestPhase testPhase, int refreshDataSetIndex, boolean isStartTime) {
    String metricName = testPhase.toString() + "_refresh_" + refreshDataSetIndex + "_";
    if (isStartTime) {
      metricName += "start";
    } else {
      metricName += "end";
    }

    return metricName;
  }
  
  // Refresh Run is executed alone without running query together
  public void addRefreshMetric(TestPhase testPhase, int refreshDataSetIndex, boolean isStartTime, Date timestamp) {
    dataMetricsMap.put(getRefreshMetricName(testPhase, refreshDataSetIndex, isStartTime), timestamp);
  }

  private String getDMMetricName(TestPhase testPhase, int refreshDataSetIndex, int dmId, boolean isStartTime) {
    String metricName = testPhase.toString() + "_dm_" + refreshDataSetIndex + "_" + dmId + "_";
    if (isStartTime) {
      metricName += "start";
    } else {
      metricName += "end";
    }

    return metricName;
  }
  
  public void addDMMetric(TestPhase testPhase, int refreshDataSetIndex, int dmId, boolean isStartTime, Date timestamp) {
    dataMetricsMap.put(getDMMetricName(testPhase, refreshDataSetIndex, dmId, isStartTime), timestamp);
  }
 
  private long getPhaseTime(TestPhase testPhase) {
    long startTime = dataMetricsMap.get(getPhaseMetricName(testPhase, true)).getTime();
    long endTime = dataMetricsMap.get(getPhaseMetricName(testPhase, false)).getTime();
    return endTime - startTime;
  } 

  public long getLoadTestTime() {
    return getPhaseTime(TestPhase.LOAD_TEST);
  }
  
  public long getPowerTestTime() {
    return getPhaseTime(TestPhase.POWER_TEST);
  }
  
  public long getThroughputTest1Time() {
    return getPhaseTime(TestPhase.THROUGHPUT_TEST_1);
  }

  public long getThroughputTest2Time() {
    return getPhaseTime(TestPhase.THROUGHPUT_TEST_2);
  }

  public long getQueryTime(TestPhase testPhase, int streamId, int queryId) {
    long startTime = dataMetricsMap.get(getQueryMetricName(testPhase, streamId, queryId, true)).getTime();
    long endTime = dataMetricsMap.get(getQueryMetricName(testPhase, streamId, queryId, false)).getTime();
    return endTime - startTime;
  } 

  public long getPowerTestQueryTime(int queryId) {
    return getQueryTime(TestPhase.POWER_TEST, 0, queryId);
  }
  
  private long getRefreshDataSetTime(TestPhase testPhase, int refreshDataSetIndex) {
    long startTime = dataMetricsMap.get(getRefreshMetricName(testPhase, refreshDataSetIndex, true)).getTime();
    long endTime = dataMetricsMap.get(getRefreshMetricName(testPhase, refreshDataSetIndex, false)).getTime();
    return endTime - startTime;
  } 

  private long getRefreshRunTime(TestPhase testPhase) {
    long time = 0;
    
    for (int i = 1; i <= conf.getNumberOfStreams() / 2; i++) {
      time += getRefreshDataSetTime(testPhase, i);
    }

    return time;
  } 

  public long getRefreshRun1Time() {
    return getRefreshRunTime(TestPhase.THROUGHPUT_TEST_1);
  }

  public long getRefreshRun2Time() {
    return getRefreshRunTime(TestPhase.THROUGHPUT_TEST_2);
  }

  public long getDMTime(TestPhase testPhase, int refreshDataSetIndex, int dmId) {
    long startTime = dataMetricsMap.get(getDMMetricName(testPhase, refreshDataSetIndex, dmId, true)).getTime();
    long endTime = dataMetricsMap.get(getDMMetricName(testPhase, refreshDataSetIndex, dmId, false)).getTime();
    return endTime - startTime;
  } 
  
  public long getQueryRun1Time() {
    return getThroughputTest1Time() - getRefreshRun1Time();
  }

  public long getQueryRun2Time() {
    return getThroughputTest2Time() - getRefreshRun2Time();
  }
  
  private long[][] getQueryRunStatistics(TestPhase testPhase) {
    int numberOfQueries = ThroughputTest.getNumberOfQueries();
    int numberOfStreams = conf.getNumberOfStreams();
    int[] sortedQueryIds = ThroughputTest.getSortedQueryIds();

    // for each query, an array of long type consists of min, median, max time
    long[][] queryRunStatistics = new long[numberOfQueries][3];
    
    for (int i = 0; i < numberOfQueries; i++) {
      long[] queryTime = new long[numberOfStreams];
      for (int j = 0; j < numberOfStreams; j++) {
        queryTime[j] = getQueryTime(testPhase, j, sortedQueryIds[i]);
      }
      Arrays.sort(queryTime);
      // min query time
      queryRunStatistics[i][0] = queryTime[0];
      // max query time
      queryRunStatistics[i][2] = queryTime[numberOfStreams - 1];
      // median query time
      // note that numberOfStreams is an even number
      queryRunStatistics[i][1] = (queryTime[numberOfStreams / 2] + queryTime[numberOfStreams / 2 - 1]) / 2;
    }
    
    return queryRunStatistics;
  }

  public long[][] getQueryRun1Statistics() {
    return getQueryRunStatistics(TestPhase.THROUGHPUT_TEST_1);
  }

  public long[][] getQueryRun2Statistics() {
    return getQueryRunStatistics(TestPhase.THROUGHPUT_TEST_2);
  }

  public double getPerformanceMetric() {
    int numberOfQueries = ThroughputTest.getNumberOfQueries();
    int S = conf.getNumberOfStreams();
    
    int Q = 3 * S * numberOfQueries;
    long Ttt = getThroughputTest1Time() + getThroughputTest2Time();
    double Tld = 0.01 * S * getLoadTestTime();
    long Tpt = getPowerTestTime() * S;
    double performanceMetric = conf.getScale() * Q * 1.0 / 
                        ((Tpt + Ttt + Tld) / (3600 * 1000));

    return performanceMetric;
  }
}

