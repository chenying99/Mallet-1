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

this="${BASH_SOURCE-$0}"
bin=$(cd -P -- "$(dirname -- "$this")" && pwd -P)
script="$(basename -- "$this")"
this="$bin/$script"

#++++++++++++ Paths and options (configurable) ++++++++++++
# basic paths
HADOOP_EXECUTABLE= 
HADOOP_CONF_DIR=
STREAMING=

# compress: 0-off, 1-on
COMPRESS_GLOBAL=
COMPRESS_CODEC_GLOBAL=
#COMPRESS_CODEC_GLOBAL=org.apache.hadoop.io.compress.DefaultCodec
#COMPRESS_CODEC_GLOBAL=com.hadoop.compression.lzo.LzoCodec
#COMPRESS_CODEC_GLOBAL=org.apache.hadoop.io.compress.SnappyCodec

# parameters for dsgen
. "$bin/../conf/conf.properties"

PARALLEL=8
REFRESHES=$numberOfStreams
# hdfs data path
DATA_HDFS=${malletDbDir}

#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

#+++++++++++++++ Set/guess paths or values ++++++++++++++++
# get conf dir if command option defined
if [ $# -gt 1 ]; then
    if [ "--hadoop_config" = "$1" ]; then
        shift
        confdir=$1
        shift
        HADOOP_CONF_DIR=$confdir
    fi
fi

# check and set basic paths
if [ -n "$HADOOP_HOME" ]; then
    HADOOP_EXECUTABLE=$HADOOP_HOME/bin/hadoop
    if [ -z $HADOOP_CONF_DIR ]; then
        HADOOP_CONF_DIR=$HADOOP_HOME/conf
    fi
else 					
    ## guess basic paths if they are not set
    if [ -z $HADOOP_EXECUTABLE ]; then
       	HADOOP_EXECUTABLE=`which hadoop`
    fi
    IFS=':'
    for d in `$HADOOP_EXECUTABLE classpath`; do
        if [ -z $HADOOP_CONF_DIR ] && [[ $d = */conf ]]; then
            HADOOP_CONF_DIR=$d
       	fi
    done
    unset IFS
fi

echo HADOOP_EXECUTABLE=${HADOOP_EXECUTABLE:? "ERROR: Please set paths in $this before using HiBench."}
echo HADOOP_CONF_DIR=${HADOOP_CONF_DIR:? "ERROR: Please set paths in $this before using HiBench."}


# check and set default HDFS path of data
if [ -z "$DATA_HDFS" ]; then
    DATA_HDFS=/mallet
fi

# check and set default compress options
if [ -z "$COMPRESS_GLOBAL" ]; then
    COMPRESS_GLOBAL=1
fi

if [ -z "$COMPRESS_CODEC_GLOBAL" ]; then
    COMPRESS_CODEC_GLOBAL=org.apache.hadoop.io.compress.DefaultCodec
fi

# internal paths (DO NOT CHANGE UNLESS NECESSARY)
#if $HADOOP_EXECUTABLE version|grep -i -q cdh4; then
#    HADOOP_VERSION=cdh4
#else
#    HADOOP_VERSION=hadoop1
#fi
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

