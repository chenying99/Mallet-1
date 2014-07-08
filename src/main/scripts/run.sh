#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

bin=`dirname "$0"`
basedir=`cd "$bin/.."; pwd`

for f in ${basedir}/lib/*.jar; do
  HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:$f;
done
export HADOOP_CLASSPATH

export HADOOP_OPTS="${HADOOP_OPTS}  -Djava.util.logging.config.file=${basedir}/conf/logging.properties"

if [ "${HADOOP_HOME}" != "" ] ; then
  ${HADOOP_HOME}/bin/hadoop com.intel.mallet.Driver "$@"
else
  hadoop com.intel.mallet.Driver "$@"
fi


