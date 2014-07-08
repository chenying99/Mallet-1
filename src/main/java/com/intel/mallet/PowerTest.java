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
import java.util.Date;

public class PowerTest {
  private static final Logger logger = Logger.getLogger(PowerTest.class.getName());
  private static int[] queryIds;
  
  public PowerTest() {
  }

  public static int[] getQueryIds() {
   return queryIds;
  }

  public void run() throws MalletException {
    logger.info("Power Test starts.");

    Metrics metrics = Metrics.getMetrics();
    metrics.addPhaseMetric(TestPhase.POWER_TEST, true, new Date());

    // Invoke dsqgen tool to generate queries for stream 0.
    String[] sqlFiles = TpcdsTool.generateStreamSqlFile(1);
    
    // We can execute the Power Test in the main thread. However,
    // because typicall it will take a long time to execute JDBC queries,
    // running the Power Test in a new thread will not block the main
    // thread, and allow the main thread to be responsive to user events
    // such as UI envents.
        
    // The queries in the Power Test shall be executed in the order
    // of stream 0.
    QueryStream queryStream = new QueryStream(TestPhase.POWER_TEST, 0, sqlFiles[0]);
    queryIds = queryStream.getQueryIds();
    queryStream.start();

    try {
      // Wait for the thread to finish
      queryStream.join();
    } catch (InterruptedException e) {
    }

    Throwable e = queryStream.getExitCause();
    if (e != null) {
      throw new MalletException(queryStream.getName() + " aborted.", e);
    }
    
    metrics.addPhaseMetric(TestPhase.POWER_TEST, false, new Date());

    logger.info("Power Test ends.");
  }
}
