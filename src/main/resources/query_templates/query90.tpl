--
-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements. See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License. You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

DEFINE COMMENT = "-- ";
[COMMENT] QUERY_ID=90;

 define DEPCNT=random(0,9,uniform);
 define HOUR_AM = random(6,12,uniform);
 define HOUR_PM = random(13,21,uniform);
 define _LIMIT=100;
 
 -- Specifying scale and precision for a Decimal is not supported by HIVE now.
-- use double for Decimal. Decimal is supported since 0.11.0
select  cast(amc as double)/cast(pmc as double) am_pm_ratio
 from ( select count(*) amc
       from 
           web_sales ws join household_demographics
             on ws.ws_ship_hdemo_sk = household_demographics.hd_demo_sk
             and household_demographics.hd_dep_count = [DEPCNT]
           join time_dim
             on ws.ws_sold_time_sk = time_dim.t_time_sk
             and time_dim.t_hour between  [HOUR_AM] and [HOUR_AM]+1
           join web_page
             on ws.ws_web_page_sk = web_page.wp_web_page_sk
             and web_page.wp_char_count between 5000 and 5200
      ) at join
      ( select count(*) pmc
       from 
           web_sales ws join household_demographics
             on ws.ws_ship_hdemo_sk = household_demographics.hd_demo_sk
             and household_demographics.hd_dep_count = [DEPCNT]
           join time_dim
             on ws.ws_sold_time_sk = time_dim.t_time_sk
             and time_dim.t_hour between [HOUR_PM] and [HOUR_PM]+1
           join web_page
             on ws.ws_web_page_sk = web_page.wp_web_page_sk
             and web_page.wp_char_count between 5000 and 5200
      ) pt
 order by am_pm_ratio
 [_LIMITC];
