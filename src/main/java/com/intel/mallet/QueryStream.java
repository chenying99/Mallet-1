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

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.sql.Connection;
import java.sql.SQLException;

public class QueryStream extends MalletThread {
  private static final Logger logger = Logger.getLogger(QueryStream.class.getName());

  private final TestPhase testPhase;
  private final int streamId;
  private final String streamSqlFile;
  private final ArrayList<String> queries = new ArrayList<String>();
  private final int[] queryIds;

  private Connection con = null;
  private Coordinator coordinator;
  
  public QueryStream(TestPhase testPhase, int streamId, String streamSqlFile)
    throws MalletException {
    assert (testPhase != TestPhase.LOAD_TEST);
    
    String threadName = testPhase.toString() + " query stream";
    if (testPhase != TestPhase.POWER_TEST) {
      threadName += (" " + streamId);
    }
    setName(threadName);
    
    this.testPhase = testPhase;
    this.streamId = streamId;
    this.streamSqlFile = streamSqlFile;
    
    // read all queries for this stream from streamSqlFile
    // and build a query array
    ArrayList<Integer> queryIdList = new ArrayList<Integer>();

    try {
      BufferedReader r = new BufferedReader(new InputStreamReader
        (new FileInputStream(streamSqlFile), "US-ASCII"));
    
      while(true) {
        String line = r.readLine();
        if (line == null) {
          break;
        }

        // skip blank lines before a query
        line = line.trim();
        if (line.isEmpty()) {
          continue;
        }
      
        // Read all lines for a query
        String query = "";
        int queryId = -1;
        while(true) {
          if (line.startsWith("--")) {
            Pattern pattern = Pattern.compile("^--\\s*QUERY_ID\\s*=\\s*(\\d+).*");
            Matcher matcher = pattern.matcher(line);
            if (matcher.matches()) {
              try {
                queryId = Integer.parseInt(matcher.group(1));
              } catch (NumberFormatException e) {
                throw new MalletException("Invalid query ID", e);
              }
            }
          } else {
            query += ("\n" + line);
          }

          line = r.readLine();
          if (line == null) {
            break;
          }

          line = line.trim();
          if(line.isEmpty()) {
            break;
          }
        }
        
        if (query.isEmpty()) {
          throw new MalletException("Query is empty");
        } else if (queryId < 0) {
          throw new MalletException("No query ID is found");
        }
        queryIdList.add(queryId);
        queries.add(query);
      }
    } catch (FileNotFoundException e) {
      throw new MalletException("Stream SQL file not found", e);
    } catch (UnsupportedEncodingException e) {
      throw new MalletException(streamSqlFile + " is not ASCII file.", e);
    } catch (IOException e) {
      throw new MalletException("Error reading " + streamSqlFile, e);
    }
    
    logger.fine("Read " + queries.size() + " queries from " + streamSqlFile);
    
    // check query number
    if (queries.size() == 0) {
      throw new MalletException("No query in SQL file: " + streamSqlFile);
    } else {
      queryIds = new int[queryIdList.size()];
      int i = 0;
      for (Integer integer : queryIdList) {
        queryIds[i] = integer.intValue();
        i++;
      }

      logger.info(getName() + " : " + queries.size() + " queries");
      logger.finer("Query[0]: " + queries.get(0));
    }
  }

  public int getNumberOfQueries() {
    return queries.size();
  }

  public int[] getQueryIds() {
    return queryIds;
  }

  public void setCoordinator(Coordinator coordinator) {
    this.coordinator = coordinator;
  }
  
  private void executeQuery(int queryIndex) throws MalletException {
    String context = getName() + " query " + (queryIndex + 1) + " (ID = " + queryIds[queryIndex] + ")";
    logger.info(context + " starts.");
    
    // A query may have multiple statements
    String query = queries.get(queryIndex);

    Metrics metrics = Metrics.getMetrics();    
    metrics.addQueryMetric(testPhase, streamId, queryIds[queryIndex], true, new Date());
    try {
      JdbcClient.executeStatements(con, query);
    } catch (MalletException e) {
      throw new MalletException(context + " failed.", e);
    } catch (SQLException e) {
      throw new MalletException(context + " failed.", e);
    }
    metrics.addQueryMetric(testPhase, streamId, queryIds[queryIndex], false, new Date());
    
    logger.info(context + " ends.");
  }
  
  public int getStreamId() {
    return streamId;
  }
  
  public String getStreamSqlFile() {
    return streamSqlFile;
  }

  private void runQueries() throws MalletException {
    // use the database for tests.

    try {
      JdbcClient.executeStatements(con, "use mallet_db;");
    } catch (SQLException e) {
      throw new MalletException(getName() + " failed to use mallet_db database.", e);
    } catch (MalletException e) {
      throw new MalletException(getName() + " failed to use mallet_db database.", e);
    }

    try {
      JdbcClient.executeStatements(con, Conf.getConf().getDbSettings());
    } catch (MalletException e) {
      throw new MalletException(getName() + " failed to apply DB settings.", e);
    } catch (SQLException e) {
      throw new MalletException(getName() + " failed to apply DB settings.", e);
    }
        
    // run all queries serially.
    for (int i = 0; i < getNumberOfQueries(); i++) {
      if (coordinator != null) {
        coordinator.queryStart();
      }
      executeQuery(i);
      if (coordinator != null) {
        coordinator.queryEnd();
      }
    }
  }

  @Override
  public void run() {
    logger.info(getName() + " starts.");

    try {
      con = JdbcClient.getConnection();
      
      runQueries();
    } catch (MalletException e) {
      setExitCause(e);
      return;
    } finally {
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

