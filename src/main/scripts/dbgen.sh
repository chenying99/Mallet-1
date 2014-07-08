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

DBGEN_HDFS_BASE=${DATA_HDFS}/mallet
DBGEN_HDFS_DATA=${DBGEN_HDFS_BASE}/DATA

if [ -n "$HADOOP_HOME" ]; then
    HADOOP_EXECUTABLE=$HADOOP_HOME/bin/hadoop
else
    HADOOP_EXECUTABLE=`which hadoop`
fi

#begin generating
while read TABLE CHILD
do
  rm -f data/child-${TABLE}-${CHILD}/*
  mkdir -p data/child-${TABLE}-${CHILD}
  [ ! -x ./dsdgen ] && chmod +x ./dsdgen

  if [[ ${TABLE} =~ ^[0-9]+$ ]]; then
    # Data generation for refresh tables

    REFRESH=${TABLE}
    echo Generate refresh data - data set: ${REFRESH}, child: ${CHILD} locally
		./dsdgen -scale ${SCALE} -dir data/child-${REFRESH}-${CHILD} -update ${REFRESH} -terminate N -parallel ${PARALLEL} -child ${CHILD} 2>&1
    if [ $? -ne 0 ]; then
      echo Eror generating refresh data - data set: ${REFRESH}, child: ${CHILD} 1>&2
      exit 1
    else
      # Move refresh data to HDFS
   	  echo Move refresh data to HDFS - data set: ${REFRESH}, child: ${CHILD}

      for RT in s_catalog_page s_zip_to_gmt s_purchase_lineitem s_customer s_customer_address s_purchase s_catalog_order s_web_order s_item s_catalog_order_lineitem s_web_order_lineitem s_store s_call_center s_web_site s_warehouse s_web_page s_promotion s_store_returns s_catalog_returns s_web_returns s_inventory
      do
        # the chunk file may be empty
        if [ -f data/child-${REFRESH}-${CHILD}/${RT}_${CHILD}_${PARALLEL}.dat ]; then  
          $HADOOP_EXECUTABLE fs -moveFromLocal data/child-${REFRESH}-${CHILD}/${RT}_${CHILD}_${PARALLEL}.dat ${DBGEN_HDFS_DATA}/${RT}_${REFRESH}/${RT}_${CHILD}_${PARALLEL}.dat
          if [ $? -ne 0 ]; then
            echo Eror moving refresh data - data set: ${REFRESH}, child: ${CHILD}, refresh table: ${RT} 1>&2
            exit 1
          fi
        fi
      done
      if [ ${CHILD} -eq 1 ]; then
        for RT in delete inventory_delete
        do
          $HADOOP_EXECUTABLE fs -moveFromLocal data/child-${REFRESH}-${CHILD}/${RT}_${REFRESH}.dat ${DBGEN_HDFS_DATA}/s_${RT}_${REFRESH}/s_${RT}_${REFRESH}.dat
          if [ $? -ne 0 ]; then
            echo Eror moving refresh data - data set: ${REFRESH}, child: ${CHILD}, refresh table: ${RT} 1>&2
            exit 1
          fi
        done
      fi
    fi

  else
    # Data generation for TPC-DS tables


    echo Generate table data - ${TABLE}, child: ${CHILD} locally
    ./dsdgen -scale ${SCALE} -dir data/child-${TABLE}-${CHILD} -table ${TABLE} -terminate N -parallel ${PARALLEL} -child ${CHILD} 2>&1
    if [ $? -ne 0 ]; then
      echo Eror generating table data - ${TABLE}, child: ${CHILD} 1>&2
      exit 2
    else
      # the chunk file may be empty
      if [ -f data/child-${TABLE}-${CHILD}/${TABLE}_${CHILD}_${PARALLEL}.dat ]; then  
        echo Move table data - ${TABLE}, child: ${CHILD} into HDFS
        $HADOOP_EXECUTABLE fs -moveFromLocal data/child-${TABLE}-${CHILD}/${TABLE}_${CHILD}_${PARALLEL}.dat ${DBGEN_HDFS_DATA}/${TABLE}/${TABLE}_${CHILD}_${PARALLEL}.dat
        if [ $? -ne 0 ]; then
          echo Eror moving table data - ${TABLE}, child: ${CHILD} 1>&2
          exit 2
        fi
      fi

      COMPANION_TABLE=""
      if [ "${TABLE}" == "web_sales" ]; then
        COMPANION_TABLE="web_returns"
      elif [ "${TABLE}" == "catalog_sales" ]; then
        COMPANION_TABLE="catalog_returns"
      elif [ "${TABLE}" == "store_sales" ]; then
        COMPANION_TABLE="store_returns"     
      fi
      if [ "${COMPANION_TABLE}" != "" ] && [ -f data/child-${TABLE}-${CHILD}/${COMPANION_TABLE}_${CHILD}_${PARALLEL}.dat ]; then
        echo Move table data - ${COMPANION_TABLE}, child: ${CHILD} into HDFS
        $HADOOP_EXECUTABLE fs -moveFromLocal data/child-${TABLE}-${CHILD}/${COMPANION_TABLE}_${CHILD}_${PARALLEL}.dat ${DBGEN_HDFS_DATA}/${COMPANION_TABLE}/${COMPANION_TABLE}_${CHILD}_${PARALLEL}.dat
        if [ $? -ne 0 ]; then
          echo Eror moving table data - ${COMPANION_TABLE}, child: ${CHILD} 1>&2
          exit 2
        fi
      fi
    fi
  fi
done

