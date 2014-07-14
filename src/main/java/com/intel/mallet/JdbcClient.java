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
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JdbcClient {
  private static final Logger logger = Logger.getLogger(JdbcClient.class.getName());
  private static final String driverName = "org.apache.hive.jdbc.HiveDriver";
  
  static {
    try {
      // Load HIVE JDBC driver.
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      throw new ExceptionInInitializerError("Failed to load HIVE JDBC driver.");
    }
  }
  
  public static Connection getConnection() throws MalletException {
    Conf conf = Conf.getConf();

    try {
      // Initiate JDBC session.
      // The <password> field value is ignored in non-secure mode.
      Connection con = DriverManager.getConnection("jdbc:hive2://" + conf.getHiveServerHost() + ":" +
                                                   conf.getHiveServerPort() + "/;auth=noSasl", conf.getUser(), "");
      return con;
    } catch (SQLException e) {
      throw new MalletException(Thread.currentThread().getName() + " failed to connect to JDBC server.", e);
    }
  }
  
  /**
   * Execute a sequence of SQL statements. Each statement can be either select or DML/DDL statement.
   */
  public static void executeStatements(Connection con, String statements) throws SQLException, MalletException {
    assert (con != null);
    
    if (statements == null) {
      return;
    }
    
    Statement stmt = con.createStatement();
    String context = Thread.currentThread().getName();
    
    // HIVE Server does not support batch execution, so we have to execute the statements one by one.
    try {
      int beginIndex = 0;
      while(beginIndex < statements.length()) {
        int endIndex = statements.indexOf(';', beginIndex);
        if (endIndex < 0) {
          throw new MalletException(context + ":statement endding ';' not found.");
        }
        String statement = statements.substring(beginIndex, endIndex).trim();

        logger.info(context + " executes statement:" + String.format("%n") +
                    "  " + statement);
        String statementLowerCase = statement.toLowerCase();
        if (statementLowerCase.startsWith("select ") || statementLowerCase.startsWith("show ")) {
          // TODO: log query result for verfication.
          ResultSet res = stmt.executeQuery(statement);
          res.close();
        } else {
          stmt.executeUpdate(statement);
        }
        beginIndex = endIndex + 1;
      }
    } finally {
      stmt.close();
    }
  }

  public static void executeStatements(String statements) throws SQLException, MalletException {
    Connection con;
    
    con = JdbcClient.getConnection();
    try {
      JdbcClient.executeStatements(con, statements);
    } finally {
      con.close();
    }
  }
}

  
  
