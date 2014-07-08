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

public class Coordinator {
  private static final Logger logger = Logger.getLogger(Coordinator.class.getName());
  
  private int queryCount;
  private int queryRunning;
  private int numberOfStreams;
  private int numberOfQueries;
  private int refreshDataSetIndex;
  private Object queryWaiting;
  private Object refreshWaiting;
  
  public Coordinator(int numberOfStreams, int numberOfQueries) {
    queryCount = 0;
    queryRunning = 0;
    assert (numberOfQueries >= 3);
    this.numberOfQueries = numberOfQueries;
    this.numberOfStreams = numberOfStreams;
    refreshDataSetIndex = 1;
    queryWaiting = new Object();
    refreshWaiting = new Object();
  }
 
  private int getRefreshPoint() {
    return 3 * numberOfStreams + 2 * (numberOfQueries - 3)  * (refreshDataSetIndex - 1);
  }
  
  public void queryStart() {
    while(true) {
      synchronized(this) {
        if (queryCount < getRefreshPoint()) {
          queryCount++;
          queryRunning++;
          return;
        }
      }

      // Wait for the completion of next refresh data set
      int currentIndex = refreshDataSetIndex;
      while (refreshDataSetIndex == currentIndex) {
        try {
          synchronized (queryWaiting) {
            queryWaiting.wait();
          }
        } catch (InterruptedException e) {
        }
      }
    }
  }
 
  private boolean isRefreshReady() {
    return (queryRunning == 0 && queryCount == getRefreshPoint());
  }
 
  public void queryEnd() {
    synchronized(this) {
      queryRunning--;
      if (!isRefreshReady()) {
        return;
      }
    }
    
    // Notify the refresh group to start next refresh data set
    synchronized (refreshWaiting) {
      refreshWaiting.notify();
    }    
  }

  public void refreshStart() {
    // Wait until number of finished queries meets the requirement
    while (!isRefreshReady()) {
      try {
        synchronized (refreshWaiting) {
          refreshWaiting.wait();
        }
      } catch (InterruptedException e) {
      }
    }
  }

  public void refreshEnd() {
    refreshDataSetIndex++;
    // Notify all waiting query streams to start
    synchronized (queryWaiting) {
      queryWaiting.notifyAll();
    }
  }  
}
