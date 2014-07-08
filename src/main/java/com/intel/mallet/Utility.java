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
import java.io.*;

public class Utility {
  private static final Logger logger = Logger.getLogger(Utility.class.getName());
  
  public static String readHqlFile(String fileName) throws MalletException {
    String hql = "";
    
    try {
      BufferedReader r = new BufferedReader(new InputStreamReader
        (new FileInputStream(fileName), "US-ASCII"));
      
      while(true) {
        String line = r.readLine();
        if (line == null) {
          break;
        }

        line = line.trim();
        if(line.isEmpty() || line.startsWith("--")) {
          continue;
        }
        
        hql += ("\n" + line);
      }        
    } catch (FileNotFoundException e) {
      throw new MalletException("Can't read file", e);
    } catch (UnsupportedEncodingException e) {
      throw new MalletException("Can't read file", e);
    } catch (IOException e) {
      throw new MalletException("Can't read file", e);
    }
    
    if (hql.isEmpty()) {
      throw new MalletException("Empty file: " + fileName);
    }
    
    return hql;
  }
}

