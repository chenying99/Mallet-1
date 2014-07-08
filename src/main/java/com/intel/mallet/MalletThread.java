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
import java.util.logging.Level;

public class MalletThread extends Thread {
  private Throwable exitCause = null;

  private static class MalletUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
    private static final Logger logger = Logger.getLogger(MalletUncaughtExceptionHandler.class.getName());
    
    public void uncaughtException(Thread t, Throwable e) {
      // ignore ThreadDeath
      if (e instanceof ThreadDeath) {
        return;
      }

      if (t instanceof MalletThread) {
        // for worker threads, indicate the exit cause
        ((MalletThread) t).setExitCause(e);
      } else {
        // main thread, abort the program with a non-zero code
        logger.log(Level.SEVERE, "Program aborted.", e);
        System.exit(1);
      }
    }
  }
   
  public static void setUncaughtExceptionHandler() {
    // Set an uncaught exception handler for all threads including the main thread.
    Thread.setDefaultUncaughtExceptionHandler(new MalletUncaughtExceptionHandler());
  }
  
  public Throwable getExitCause() {
    if (isAlive()) {
      throw new IllegalThreadStateException("Thread has not exited");
    }
    return exitCause;
  }
  
  public void setExitCause(Throwable exitCause) {
    this.exitCause = exitCause;
  }
}

