# Mallet - A Decision Support Benchmark Derived from TPC-DS

- Release version: 1.0
- Release date: 2014/7/14
- Contact: [Rui Sun](mailto:rui.sun@intel.com), [Lan Yi](mailto:lan.yi@intel.com), [Hao Cheng](mailto:hao.cheng@intel.com), [Jiangang, Duan](mailto:jiangang.duan@intel.com)
- Homepage: https://github.com/intel-hadoop/Mallet

- Contents:
    1. Overview
    2. Getting Started
    3. Project Directory Layout
    4. Limitations and Known Issues

---
## OVERVIEW

Mallet is an open source decision support benchmark for HiveQL-compatible SQL engines, which is derived from [the TPC-DS benchmark](http://www.tpc.org/tpcds/default.asp). It basically follows the TPC-DS's modeling of several generally applicable aspects of a decision support system, including the database schema, data population, queries, data maintenance. Note that the result of Mallet is not comparable to any published TPC-DS Benchmark results.

Mallet implements the Driver and follows the benchmark procedure defined in [the TPC-DS 1.1.0 specification](http://www.tpc.org/tpcds/spec/tpcds_1.1.0.pdf). Mallet performs the Load Test, the Power Test and the Throughput Test 1 & 2. It reports part of the metric indicators defined in the specification. A text based report with the primary performance metric (QphM@SF) will be generated upon the completion of the benchmark.

Mallet utilizes the TPC-DS toolkit for data preparation, i.e., generation of the table and maintenance data. To make the data generation fast, Mallet takes advantage of the Hadoop streaming and the data chunk generation support by the DSDGEN tool for distributed data generation. The generated data is stored in HDFS for accesses. For HIVE and SHARK, the data will be loaded in place as external tables.

Mallet's workload consists of 65 queries in HiveQL which are converted from the corresponding TPC-DS queries in SQL. Mallet's workload also implements in HiveQL all of the data maintenance functions defined in the TPC-DS 1.1.0 specification.

Mallet uses JDBC to present workload to the target database, so it works with HiveQL-compatible database applications with JDBC support, such as HIVE,Shark.

---
## Getting Started

### Prerequisites
  * Java 1.6

  * The TPC-DS Toolkit

  You need to download the TPC-DS software package from [the TPC webiste](http://www.tpc.org/tpcds/dsgen-download-request.asp). Follow the guide in the package to build the DSQGEN and DSDGEN tools from the source code contained in the package. Make sure the binaries built from the source can execute on each node of your Hadoop cluster without problems caused by differences of operating environments, for example, GLIBC version.

  * Hadoop 1.x or 2.x

    Make sure you set the `HADOOP_HOME` environment variable or put the `<HADOOP Home>/bin` directory in the PATH environment variable.  
    For Hadoop 1.x, only Hadoop 1.0.4 was tested.  
    For Hadoop 2.x, only CDH 5.0 beta YARN mode was tested.

### Requirements on Target Databases
#### HIVE

  If you want to run Mallet with HIVE as the target database, HIVE 0.13.1 or 0.12.0 is required. HIVE 0.13.1 is the default target.

  * HIVE 0.13.1

  You need to download the source package of HIVE 0.13.1 from [http://hive.apache.org/downloads.html](http://hive.apache.org/downloads.html) and apply the patch file (`src/main/resources/hive/hive-0.13.1.patch`), then build the HIVE binary.
  
  * HIVE 0.12.0

  You need to download the source package of HIVE 0.12.0 from [http://hive.apache.org/downloads.html](http://hive.apache.org/downloads.html) and apply the patch file (`src/main/resources/hive/hive-0.12.0.patch`), then build the HIVE binary.

  Then you need to modify `pom.xml` to changet the version for the dependency `hive-jdbc` to 0.12.0. Then build Mallet.
  
#### SHARK

  If you want to run Mallet with SHARK as the target database, SHARK 0.9.0 is required.

### Install Mallet

  1. Checkout the source code of Mallet in the open source repository to your local directory.
  2. In your local Mallet directory, type `mvn clean package` to build Mallet.

     Note: If you intend to run Mallet with SHARK, change the version of the `hive-jdbc` dependency in pom.xml from 0.12.0 to 0.11.0.

  3. After build, the Mallet binary can be found at `target/mallet-1.0-bin/mallet-1.0/`.

### Configure Mallet

  * Copy the TPC-DS Tool Binaries

    It seems the DSQGEN tool has problem with long path, so the workaround is to copy 3 TPC-DS tool binaries you built before (`dsdgen, dsqgen, tpcds.idx`) to the `tools` sub-directory of `<Mallet binary directory>`.

  * Configure Benchmark Parameters

    You can modify benchmark parameters in `<Mallet binary directory>/conf/conf.properties`:

          hiveServerHost     < The host name of the target databases's JDBC service >
          hiveServerPort     < The port number of the target databases's JDBC service >
          numberOfStreams    < The number of query streams in the Throughput Tests >
          scaleFactor        < The scale factor. Valid options are 1, 100, 300, 1000, 3000, 10000, 30000, 100000 >
          user               < The username used to connect to JDBC >
          malletDbDir        < The HDFS root directory for the Mallet data >

  
  * Configure Data Preparation

    You need to set some global environment variables in `<Mallet binary directory>/bin/config.sh`:

          PARALLEL               < The number of data chunks generated in parallel >
          HADOOP_EXECUTABLE      < The Hadoop executable location. Optional, set if it can't be automatically discovered >
          HADOOP_CONF_DIR        < The hadoop configuration directory. Optional, set if it can't be automatically discovered >
          STREAMING              < The path to Hadoop streaming jar. Optional, set if it can't be automatically discovered >
          COMPRESS_GLOBAL        < Switch on/off the compression for the generated data, 0 is disable, 1 is enable. Optional >
          COMPRESS_CODEC_GLOBAL  < The default codec used for data compression. Optional >

    Note:
      1. Mallet will guess the value of these variables if they are not explicitly set. If so, Mallet guarantees neither the correctness of guess nor the success running of benchmarks.
      2. Do not change the default values of other global environment variables unless necessary.

### Run Mallet
  1. cd `<Mallet binary directory>`
  2. Make sure Hadoop is running. type `bin/prepare.sh` to generate table and maintenance data.

     You may try to increase the PARALLEL variable (the recommended value is (Map task capacity of the cluster*2)/(21+numberOfStreams)) to reduce the duration of data generation.

  3. Make sure the target database is running. type `bin/run.sh` to start the benchmark.

  `bin/run.sh` without any command line options performs a complete benchmark. In some cases, you can provide one of the following command line options to alter the behavior:

        --quickrun          Performs the benchmark with empty query and data maintenance operations. Used to facilitate development and verify installation and environment settings.
        --powertest         Performs only power test.
        --query <query id>  Performs only a single query.

  4. Upon the completion of the benchmark, a report with the primary performance metric (QphM@SF) will be generated in the stdout.

  The following is an abbreviated sample report:

        ------------------- Mallet Benchmark Report --------------------
        Number of Query Streams:           4
        Number of queries in Query Stream: 65

        Database Load Elapsed Time:        0h:0m:7s
        Power Test Elapsed Time:           4h:24m:51s
        Throughput Run 1 Elapsed Time:     6h:24m:24s
        Query Run 1 Elapsed Time:          5h:14m:10s
        Refresh Run 1 Elapsed Time:        1h:10m:13s
        Throughput Run 2 Elapsed Time:     6h:17m:7s
        Query Run 2 Elapsed Time:          5h:6m:58s
        Refresh Run 2 Elapsed Time:        1h:10m:8s
        ----------------------------------------------------------------
        Performance Metric = 25.700481928957096 QphM@1GB
        ----------------------------------------------------------------
        ---------- Query Run 1 Timing Intervals (in seconds) -----------
        Query   Minimum         Median          Maximum
        2       225.712         235.046         247.064
        ...
        98      157.288         203.679         217.967
        ---------- Query Run 2 Timing Intervals (in seconds) -----------
        Query   Minimum         Median          Maximum
        2       201.364         215.648         227.006
        ...
        98      158.62          210.739         220.181

---
## Project Directory Layout

    src/main
      |- java                 -- The Mallet benchmark driver source code implemented in JAVA.
      |- config               -- The configurations for the benchmark, such as number of streams, Scale Factor, database JDBC server and port, …
      |- scripts              -- The Shell scripts for data preparation and benchmark running.
      |- resources 
           |- dm_function     -- DM functions in HiveQL for Data Maintenance.
           |- hive            -- HiveQL scripts to create TPC-DS tables and refresh tables.
           |- query_templates -- query templates in HiveQL.

---
## Limitations and Known Issues

* Mallet contains only 65 queries, which is a subset of all 99 queries defined in the TPC-DS 1.1.0 specification.
* Mallet relaxes the ACID requirements on the target databases in the TPC-DS 1.1.0 specification.
* Mallet does not report price related metrics defined in the TPC-DS 1.1.0 specification.


