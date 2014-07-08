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

import java.util.Date;
import java.util.logging.Logger;
import java.util.HashMap;
import java.sql.*;
import java.io.*;

public class RefreshGroup extends MalletThread {
  private static final Logger logger = Logger.getLogger(RefreshGroup.class.getName());

  private final TestPhase testPhase;
  private final int numberOfRefreshSets;
  private final int refreshGroupNumber;
  private Connection con;
  private Coordinator coordinator;
  // Per the TPC-DS spec, the execution order of the DM functions can be freely chosen.
  // Note we deliberately run type-3 DM functions after type-4 and type-5 ones, because after
  // the execution of the latter ones, 
  //   the catalog_sales & catalog_returns & store_sales & store_returns & web_sales & web_returns & inventory
  //   tables are no longer external tables but managed tables, so the original table data will be intact, which means
  //   that no data re-generation is required for repetitive executions of this benchmark.
  private final String[] DMFunctionSequence = {"DM_CP", "DM_C", "DM_CA", "DM_P", "DM_W", 
                                               "DM_CC", "DM_I", "DM_S", "DM_WP", "DM_WS",
                                               "DF_CS", "DF_SS", "DF_WS",
                                               "DF_I",
                                               "LF_CR", "LF_CS", "LF_I", "LF_SR", "LF_SS", "LF_WR", "LF_WS"};
  private final int[] DMFunctionType = {1, 1, 1, 1, 1,
                                        2, 2, 2, 2, 2,
                                        4, 4, 4,
                                        5,
                                        3, 3, 3, 3, 3, 3, 3};
  private final int[] DMFunctionId = {1, 2, 3, 4, 5,
                                      6, 7, 8, 9, 10,
                                      18, 19, 20,
                                      21,
                                      11, 12, 13, 14, 15, 16, 17};

  // queries for each DM function of type 2 to get row key base.
  private final HashMap<String, String> getRowKeyBase;
                                                                                     
  public RefreshGroup(TestPhase testPhase, Coordinator coordinator) {
    if (testPhase == TestPhase.THROUGHPUT_TEST_1) {
      refreshGroupNumber = 1;
    } else {
      refreshGroupNumber = 2;
    }
    setName(testPhase.toString() + " refresh group");
    
    this.testPhase = testPhase;
    numberOfRefreshSets = Conf.getConf().getNumberOfStreams() / 2;
    
    assert(coordinator != null);
    this.coordinator = coordinator;
    
    getRowKeyBase= new HashMap<String, String>();
    getRowKeyBase.put("DM_CC", "select max(cc_call_center_sk) from call_center");
    getRowKeyBase.put("DM_I", "select max(i_item_sk) from item");
    getRowKeyBase.put("DM_S", "select max(s_store_sk) from store");
    getRowKeyBase.put("DM_WP", "select max(wp_web_page_sk) from web_page");
    getRowKeyBase.put("DM_WS", "select max(web_site_sk) from web_site");
  }
  
  private void doRefreshData(int refreshDataSetIndex) throws MalletException {
    String context = getName() + " refresh data set " + refreshDataSetIndex;
    logger.info(context + " starts");
  
    // Loading referesh data set is timed
    Metrics metrics = Metrics.getMetrics();
    metrics.addRefreshMetric(testPhase, refreshDataSetIndex, true, new Date());

    // Load referesh data set for this iteration
    String fileSuffix = Integer.toString((refreshGroupNumber - 1) * numberOfRefreshSets + refreshDataSetIndex);

    Conf conf = Conf.getConf();

    String file = conf.getBaseDirectory() + "/hive/create_refresh.hive";
    String hql = Utility.readHqlFile(file);
    
    hql = hql.replaceAll("\\$\\{MALLET_DB_DIR\\}", conf.getMalletDbDirectory());
    hql = hql.replaceAll("\\$\\{REFRESH\\}", fileSuffix);
    
    try {
      // Creat TPC-DS refresh tables.
      JdbcClient.executeStatements(con, hql);
    } catch (SQLException e) {
      throw new MalletException("Load Test failed to create refresh tables.", e);
    } catch (MalletException e) {
      throw new MalletException("Load Test failed to create refresh tables.", e);
    }
      
    // Per the TPC-DS spec, the DM functions in each referesh set may be run sequentially or in parallel.
    // Here we run all data maintenance functions sequentially.

    for (int i = 0; i < DMFunctionSequence.length; i++) {
      // Load the DM function from file
           
      String dmDirectory = conf.getBaseDirectory() + "/dm_functions/";
      if (conf.isQuickRunMode()) {
        dmDirectory += "quickrun/";
      }

      String dmFile = dmDirectory + DMFunctionSequence[i] + ".hql";
      String dmFunction = Utility.readHqlFile(dmFile);
        
      int methodType = DMFunctionType[i];
        
      if ((methodType == 4 || methodType == 5) && !conf.isQuickRunMode()) {
        // Get 3 pairs of date range [date1,date2]
        String query;
        if (methodType == 4) {
          query = "select * from s_delete limit 3";
        } else {
          query = "select * from s_inventory_delete limit 3";
        }
        
        Statement stmt = null;
        ResultSet res = null;
        
        try {
          stmt = con.createStatement();
          res = stmt.executeQuery(query);
                              
          for (int j = 1; j <= 3; j++) {
            if (!res.next()) {
              throw new MalletException("No enough pairs of date range.");
            }
            
            String date1 = res.getString(1);
            String date2 = res.getString(2);
            logger.info("date1 = " + date1 +" ,date2 = " + date2);
            dmFunction = dmFunction.replaceAll("\\$date1_" + j, date1);
            dmFunction = dmFunction.replaceAll("\\$date2_" + j, date2);
          }
        } catch (SQLException e) {
          throw new MalletException("Failed to get 3 pairs of date range [date1,date2].", e);
        } catch (MalletException e) {
          throw new MalletException("Failed to get 3 pairs of date range [date1,date2].", e);
        } finally {
          try {
            if (res != null) {
              res.close();
            }
            if (stmt != null) {
              stmt.close();
            }
          } catch (SQLException e) {
          }
        }
      } else if (methodType == 2 && !conf.isQuickRunMode()) {
        String query = getRowKeyBase.get(DMFunctionSequence[i]);

        Statement stmt = null;
        ResultSet res = null;
        
        try {
          stmt = con.createStatement();
          res = stmt.executeQuery(query);
                              
          if (!res.next()) {
            throw new MalletException("No result from " + DMFunctionSequence[i]);
          }
          
          long rowKeyBase = res.getLong(1);
          logger.info("Row key base is " + rowKeyBase + " for " + DMFunctionSequence[i]);
          dmFunction = dmFunction.replaceAll("\\$\\{ROW_KEY_BASE\\}", Long.toString(rowKeyBase));
        } catch (SQLException e) {
          throw new MalletException("Can't get row key base for " + DMFunctionSequence[i], e);
        } catch (MalletException e) {
          throw new MalletException("Can't get row key base for " + DMFunctionSequence[i], e);
        } finally {
          try {
            if (res != null) {
              res.close();
            }
            if (stmt != null) {
              stmt.close();
            }
          } catch (SQLException e) {
          }
        }        
      }
      
      // DM Function ID is 1-based.
      String dmContext = context + " DM Function " + DMFunctionId[i] + "(" + DMFunctionSequence[i] + ")";
      logger.info(dmContext + " starts.");

      try {
        metrics.addDMMetric(testPhase, refreshDataSetIndex, DMFunctionId[i], true, new Date());
        
        // Execute the DM function
        JdbcClient.executeStatements(con, dmFunction);

        metrics.addDMMetric(testPhase, refreshDataSetIndex, DMFunctionId[i], false, new Date());
      } catch (SQLException e) {
        throw new MalletException(dmContext + " failed.", e);
      } catch (MalletException e) {
        throw new MalletException(dmContext + " failed.", e);
      }

      logger.info(dmContext + " ends.");
    }

    metrics.addRefreshMetric(testPhase, refreshDataSetIndex, false, new Date());
    
    logger.info(context + " ends");
  }

  @Override
  public void run() {
    logger.info(getName() + " starts.");
    
    try {
      // Establish connection to the target database
      con = JdbcClient.getConnection();

      try {
        JdbcClient.executeStatements(con, Conf.getConf().getDbSettings());
      } catch (MalletException e) {
        throw new MalletException(getName() + " failed to apply DB settings.", e);
      } catch (SQLException e) {
        throw new MalletException(getName() + " failed to apply DB settings.", e);
      }
          
      for (int i = 1; i <= numberOfRefreshSets; i++) {
        coordinator.refreshStart();
        doRefreshData(i);
        coordinator.refreshEnd();
      }
    } catch (MalletException e) {
      setExitCause(e);
      return;
    } finally {
      // Close connection to the target database
      if (con != null) {
        try {
          con.close();
        } catch (SQLException e) {
        }
      }
    }

    logger.info(getName() + " ends.");
  }
}
