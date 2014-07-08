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
import java.util.Properties;
import java.io.*;

// Configuration parameters.
public class Conf {
  private static final Logger logger = Logger.getLogger(Conf.class.getName());
  private static Conf conf = new Conf();

  private final String baseDirectory; //Base directory of this benchmark
  private final String hiveServerHost;
  private final String hiveServerPort;
  private final int numberOfStreams;
  private final String tpcDsToolDirectory;
  private final String tempDirectory;
  private final String malletDbDir;
  private final int scale;
  private final String user;
  private boolean quickRunMode = false;
  private boolean powerTestOnly = false;
  private boolean singleQueryMode = false;
  private int queryId;
  private String dbSettings;
  
  private String getProperty(Properties prop, String key) {
    String value = prop.getProperty(key);
    if (value == null) {
      throw new ExceptionInInitializerError(key + " in conf file not found!");
    }
    return value;
  }
  
  private Conf() {
    baseDirectory = System.getProperty("user.dir");
    tempDirectory = System.getProperty("java.io.tmpdir");
    tpcDsToolDirectory = baseDirectory + "/tools";

    String confFile = baseDirectory + "/conf/conf.properties";
    Properties prop = new Properties();
    try {
      FileInputStream in = new FileInputStream(confFile);
      prop.load(in);
    } catch (FileNotFoundException e) {
      throw new ExceptionInInitializerError(e);
    } catch (IOException e) {
      throw new ExceptionInInitializerError(e);
    }
    
    hiveServerHost = getProperty(prop, "hiveServerHost");
    hiveServerPort = getProperty(prop, "hiveServerPort");
    numberOfStreams = Integer.parseInt(getProperty(prop, "numberOfStreams"));
    // Multiple query streams are concurrently executed in a Throughput Test.
    // The number of streams is any even number larger or equal to 4.
    if (!(numberOfStreams >= 4 && ((numberOfStreams % 2) == 0))) {
      throw new ExceptionInInitializerError("Number of streams for Throughput Test must be any even number larger or equal to 4.");
    }

    scale = Integer.parseInt(getProperty(prop, "scaleFactor"));
    // Valid scale factors are 1,100,300,1000,3000,10000,30000,100000
    int[] scaleFactors = {1, 100, 300, 1000, 3000, 10000, 30000, 100000};
    int i;
    for (i = 0; i < scaleFactors.length; i++) {
      if (scale == scaleFactors[i]) {
        break;
      }
    }
    if (i >= scaleFactors.length) {
      throw new ExceptionInInitializerError("Invalid scale factor.");
    }
    
    user = getProperty(prop, "user");
    malletDbDir = getProperty(prop, "malletDbDir") + "/mallet/DATA";
  }
  
  public void parseCommandLine(String[] args) throws MalletException {
    boolean argError = false;
    
    for (int i = 0; i < args.length; i++) {
      String arg = args[i];
      if (arg.equalsIgnoreCase("--quickrun")) {
        quickRunMode = true;
      } else if (arg.equalsIgnoreCase("--powertest")) {
        powerTestOnly = true;
      } else if (arg.equalsIgnoreCase("--query")) {
        powerTestOnly = true;
        singleQueryMode = true;
        if ((i + 1) >= args.length) {
          argError = true;
          break;
        }
        arg = args[i + 1];
        try {
          queryId = Integer.parseInt(arg);
        } catch (NumberFormatException e) {
          argError = true;
          break;
        }
        if (queryId < 1 || queryId > 99) {
          argError = true;
          break;
        }
        i++;
      } else {
        argError = true;
        break;
      }
    }
    if (argError) {
      throw new MalletException("Invalid command line arguments.");
    }
  }
  
  public static Conf getConf() {
    return conf;
  }

  public String getBaseDirectory() {
    return baseDirectory;
  }
  
  public String getHiveServerHost() {
    return hiveServerHost;
  }
  
  public String getHiveServerPort() {
    return hiveServerPort;
  }
  
  public int getNumberOfStreams() {
    return numberOfStreams;
  }
  
  public String getTpcDsToolDirectory() {
    return tpcDsToolDirectory;
  }
  
  public String getTempDirectory() {
    return tempDirectory;
  }

  public String getMalletDbDirectory() {
    return malletDbDir;
  }
  
  public int getScale() {
    return scale;
  }

  public String getUser() {
    return user;
  }
  
  public boolean isQuickRunMode() {
    return quickRunMode;
  }

  public boolean isPowerTestOnly() {
    return powerTestOnly;
  }

  public boolean isSingleQueryMode() {
    return singleQueryMode;
  }

  public int getQueryId() {
    return queryId;
  }
  
  public String getDbSettings() {
    if (dbSettings != null) {
      return dbSettings;
    }
    
    String dbSettingsFile = getBaseDirectory() + "/conf/hive_settings.hql";
    
    try {
      dbSettings = Utility.readHqlFile(dbSettingsFile);
      return dbSettings;
    } catch (MalletException e) {
      return null;
    }
  }
}
