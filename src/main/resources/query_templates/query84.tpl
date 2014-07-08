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
[COMMENT] QUERY_ID=84;

 define CITY = dist(cities, 1, large);
 define INCOME = random(0, 70000, uniform);
 define _LIMIT=100;
 
 select  c_customer_id as customer_id
       ,concat(c_last_name,', ',c_first_name) as customername
 from
    customer c join customer_address ca
      on ca.ca_city         =  '[CITY]'
      and c.c_current_addr_sk = ca.ca_address_sk
    join customer_demographics cd
      on cd.cd_demo_sk = c.c_current_cdemo_sk
    join household_demographics hd
      on hd.hd_demo_sk = c.c_current_hdemo_sk
    join income_band ib
      on ib.ib_lower_bound   >=  [INCOME]
      and ib.ib_upper_bound   <=  [INCOME] + 50000
      and ib.ib_income_band_sk = hd.hd_income_band_sk
    join store_returns sr
      on sr.sr_cdemo_sk = cd.cd_demo_sk
 order by customer_id
 [_LIMITC];
 

