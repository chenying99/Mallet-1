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
import java.sql.SQLException;

public class LoadTest {
  private static final Logger logger = Logger.getLogger(LoadTest.class.getName());
  
  public LoadTest() {
  }

  public void run() throws MalletException {
    logger.info("Load Test starts.");

    Metrics metrics = Metrics.getMetrics();
    metrics.addPhaseMetric(TestPhase.LOAD_TEST, true, new Date());

    Conf conf = Conf.getConf();
           
    String file = conf.getBaseDirectory() + "/hive/create_base.hive";
    String hql = Utility.readHqlFile(file);
    
    hql = hql.replaceAll("\\$\\{MALLET_DB_DIR\\}", conf.getMalletDbDirectory());
    
    try {
      // Creat TPC-DS tables.
      JdbcClient.executeStatements(hql);
    } catch (SQLException e) {
      throw new MalletException("Load Test failed to create TPC-DS tables.", e);
    } catch (MalletException e) {
      throw new MalletException("Load Test failed to create TPC-DS tables.", e);
    }
      
    metrics.addPhaseMetric(TestPhase.LOAD_TEST, false, new Date());

    logger.info("Load Test ends.");
  }
}

