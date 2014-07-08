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

# use mallet root as working directory 
# use absolute path in all places

MALLET_HOME=$(cd $(dirname "$0")/..;pwd)

# MALLET configuration
. "$MALLET_HOME/bin/config.sh"

# check for existence of hadoop streaming
if [ -n "$HADOOP_HOME" ]; then
    # for hadoop 1.0.x
    if [ -z "$STREAMING" ] && [ -e "$HADOOP_HOME/contrib/streaming/hadoop-streaming-*.jar" ]; then
        STREAMING=$HADOOP_HOME/contrib/streaming/hadoop-streaming-*.jar
    fi
    # for hadoop 2.0.x
    if [ -z "$STREAMING" ] && [ -e "$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar" ]; then
        STREAMING=$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar
    fi
    # for other hadoop version
    if [ -z "$STREAMING" ]; then
        STREAMING=`find $HADOOP_HOME -name hadoop-stream*.jar -type f`
    fi
else
    # for hadoop 1.0.x
    if [ -z "$STREAMING" ] && [ -e `dirname ${HADOOP_EXECUTABLE}`/../contrib/streaming/hadoop-streaming-*.jar ]; then
        STREAMING=`dirname ${HADOOP_EXECUTABLE}`/../contrib/streaming/hadoop-streaming-*.jar
    fi
    # for hadoop 2.0.x
    if [ -z "$STREAMING" ] && [ -e `dirname ${HADOOP_EXECUTABLE}`/../share/hadoop/tools/lib/hadoop-streaming-*.jar ]; then
        STREAMING=`dirname ${HADOOP_EXECUTABLE}`/../share/hadoop/tools/lib/hadoop-streaming-*.jar
    fi
fi

if [ -z "$STREAMING" ]; then
    echo 'Can not find hadoop-streaming jar file, please set STREAMING path.'
    exit
fi

DSGEN_HOME="$MALLET_HOME"
# check for existence of dsgen tool
if [ -z $DSGEN_HOME ] ; then
    echo "Please set DSGEN_HOME in conf/configure.sh."
    exit
elif [ ! -d $DSGEN_HOME ] ; then
    echo "$DSGEN_HOME doesn't exist or is not a directory."
    exit
fi

cd $MALLET_HOME

#################################
# generate table and refresh data
#################################

DBGEN_LOCAL_DIR=${MALLET_HOME}/dbgen
DBGEN_HDFS_BASE=${DATA_HDFS}/mallet
DBGEN_HDFS_INPUT=${DBGEN_HDFS_BASE}/Input
DBGEN_HDFS_OUTPUT=${DBGEN_HDFS_BASE}/Output
DBGEN_HDFS_DATA=${DBGEN_HDFS_BASE}/DATA

echo "Data directory in HDFS: " $DBGEN_HDFS_DATA

echo "Preparations for data generation"

# prepare mapreduce input
if [ -d "${DBGEN_LOCAL_DIR}/Input" ]
then
  rm -f ${DBGEN_LOCAL_DIR}/Input/*
else
  mkdir -p ${DBGEN_LOCAL_DIR}/Input
fi

for TABLE in customer customer_address customer_demographics inventory web_sales catalog_sales catalog_page store_sales date_dim time_dim call_center income_band household_demographics item warehouse promotion reason ship_mode store web_site web_page;do
  for CHILD in $(seq ${PARALLEL});do
    echo "${TABLE} ${CHILD}" > ${DBGEN_LOCAL_DIR}/Input/${TABLE}-${CHILD}.txt
  done
done

for REFRESH in $(seq ${REFRESHES});do
  for CHILD in $(seq ${PARALLEL});do
    echo "${REFRESH} ${CHILD}" > ${DBGEN_LOCAL_DIR}/Input/${REFRESH}-${CHILD}.txt
  done
done

# clean up previous data if available
if ${HADOOP_EXECUTABLE} fs -test -e ${DBGEN_HDFS_BASE} ; then
  ${HADOOP_EXECUTABLE} fs -rmr ${DBGEN_HDFS_BASE}
fi

${HADOOP_EXECUTABLE} fs -mkdir ${DBGEN_HDFS_BASE}
${HADOOP_EXECUTABLE} fs -mkdir ${DBGEN_HDFS_DATA}
${HADOOP_EXECUTABLE} fs -moveFromLocal ${DBGEN_LOCAL_DIR}/Input ${DBGEN_HDFS_INPUT}

for TABLE in date_dim time_dim call_center income_band household_demographics item warehouse promotion reason ship_mode store web_site web_page customer customer_address customer_demographics inventory web_sales web_returns catalog_sales catalog_returns catalog_page store_sales store_returns
do
  ${HADOOP_EXECUTABLE} fs -mkdir ${DBGEN_HDFS_DATA}/${TABLE} &
done
wait

for REFRESH in $(seq ${REFRESHES});do
  for RT in s_catalog_page s_zip_to_gmt s_purchase_lineitem s_customer s_customer_address s_purchase s_catalog_order s_web_order s_item s_catalog_order_lineitem     s_web_order_lineitem s_store s_call_center s_web_site s_warehouse s_web_page s_promotion s_store_returns s_catalog_returns s_web_returns s_inventory s_delete s_inventory_delete
  do
    ${HADOOP_EXECUTABLE} fs -mkdir ${DBGEN_HDFS_DATA}/${RT}_${REFRESH} &
  done
  wait
done
${HADOOP_EXECUTABLE} fs -chmod -R 777 ${DBGEN_HDFS_DATA}

echo "Data generation starts at " `date`
SECOND_BEGIN=`date "+%s"`

# generate TPC-DS table data and refresh data
OPTION="-D mapred.reduce.tasks=0 \
-D mapred.job.name=prepare_mallet_db \
-D mapred.task.timeout=0 \
-input ${DBGEN_HDFS_INPUT} \
-output ${DBGEN_HDFS_OUTPUT} \
-mapper ${MALLET_HOME}/bin/dbgen.sh \
-file ${DSGEN_HOME}/tools/dsdgen -file ${MALLET_HOME}/bin/dbgen.sh -file ${DSGEN_HOME}/tools/tpcds.idx -cmdenv DATA_HDFS=${DATA_HDFS} -cmdenv SCALE=${scaleFactor} -cmdenv PARALLEL=${PARALLEL} -cmdenv REFRESHES=${REFRESHES}"

${HADOOP_EXECUTABLE} jar ${STREAMING} ${OPTION}

echo "Data generation ends at " `date`
SECOND_END=`date "+%s"`

HOURS=$(($(($SECOND_END - $SECOND_BEGIN)) / 3600))
MINUTES=$(($(($SECOND_END - $SECOND_BEGIN - $HOURS * 3600)) / 60))
SECONDS=$(($SECOND_END - $SECOND_BEGIN - $HOURS * 3600 - $MINUTES * 60))

echo "Time for Data generation: " "${HOURS}h${MINUTES}m${SECONDS}s"

