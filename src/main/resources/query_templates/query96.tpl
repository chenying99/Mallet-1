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
[COMMENT] QUERY_ID=96;

Define HOUR= text({"20",1},{"15",1},{"16",1},{"8",1});
Define DEPCNT=random(0,9,uniform);
define _LIMIT=100;

select  count(*) as cnt
from
    store_sales ss join household_demographics
      on ss.ss_hdemo_sk = household_demographics.hd_demo_sk 
      and household_demographics.hd_dep_count = [DEPCNT]
    join time_dim
      on ss.ss_sold_time_sk = time_dim.t_time_sk  
      and time_dim.t_hour = [HOUR]
      and time_dim.t_minute >= 30
    join store
      on ss.ss_store_sk = store.s_store_sk
      and store.s_store_name = 'ese'
order by cnt
[_LIMITC];
