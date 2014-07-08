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
import java.io.File;
import java.io.IOException;

public class TpcdsTool {
  private static final Logger logger = Logger.getLogger(TpcdsTool.class.getName());
  
  public static String[] generateStreamSqlFile(int numberOfStreams) throws MalletException {
    Conf conf = Conf.getConf();
    
    // Build cmd string
    
    // /*WORKAROUND*/
    // String templateDirectory = conf.getBaseDirectory() + "/query_templates";
    // It seems the dsqgen tool has problem with long path time, so workaround here is
    // that the tool is under the tool directory of the program base directory.
    String templateDirectory = "../query_templates"; 
    if (conf.isQuickRunMode()) {
      templateDirectory += "/quickrun";
    }
    String templateListFile = templateDirectory + "/templates.lst";
    String outputDirectory = conf.getTempDirectory();
    
    String cmd = "./dsqgen -INPUT " + templateListFile + " -DIRECTORY " + templateDirectory +
                 " -OUTPUT_DIR " + outputDirectory + " -DIALECT hive -STREAMS " +
                 numberOfStreams + " -SCALE " + conf.getScale();
    if (conf.isSingleQueryMode()) {
      cmd += " -TEMPLATE query" + conf.getQueryId() + ".tpl";
    }

    // Invoke the TPC-DS tool to generate queries from templates
    
    logger.info("Invoke TPC-DS tool to generate queries from templates:");
    logger.info("  " + cmd);
    Process toolProcess;
    try {
      toolProcess = Runtime.getRuntime().exec(cmd, null, new File(conf.getTpcDsToolDirectory()));
    } catch (IOException e) {
      throw new MalletException("Failed to invoke TPC-DS tool.", e);
    }
    
    // Wait for the termination of the tool process
    try {
      toolProcess.waitFor();
    } catch (InterruptedException e) {
    }
      
    // Check if the tool process has any error
    if(toolProcess.exitValue() != 0) {
      throw new MalletException("TPC-DS tool exited with error.");
    }
    
    // return the SQL file names for each stream
    String[] sqlFileNames = new String[numberOfStreams];
    for(int i = 0; i < numberOfStreams; i++) {
      String sqlFileName = outputDirectory + "/query_" + i + ".sql";
      sqlFileNames[i] = sqlFileName;
      // Make sure the file exists
      if (!(new File(sqlFileName)).exists()) {
        throw new MalletException("TPC-DS tool succeeded, but can't find " + sqlFileName);
      }
    }
    return sqlFileNames;
  }

  public static void generateRefreshDataSets() throws MalletException {
    Conf conf = Conf.getConf();
    // TODO
  }
}
