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
[COMMENT] QUERY_ID=48;

 define MS= ulist(dist(marital_status, 1, 1), 3);
 define ES= ulist(dist(education, 1, 1), 3);
 define STATE= ulist(dist(fips_county, 3, 1), 9);
 define YEAR= random(1998,2002, uniform);

 select sum (ss_quantity)
 from
   store_sales ss join store s
     on s.s_store_sk = ss.ss_store_sk
   join customer_demographics cd
     on cd.cd_demo_sk = ss.ss_cdemo_sk
     and cd.cd_marital_status = '[MS.1]'
     and cd.cd_education_status = '[ES.1]'
   join customer_address ca
     on ss.ss_addr_sk = ca.ca_address_sk
     and ca.ca_country = 'United States'
   join date_dim d
     on ss.ss_sold_date_sk = d.d_date_sk
     and d.d_year = [YEAR]
 where
 (
   ss_sales_price between 100.00 and 150.00  
 or
   ss_sales_price between 50.00 and 100.00   
 or 
   ss_sales_price between 150.00 and 200.00  
 )
 and
 (
 (
  ca_state in ('[STATE.1]', '[STATE.2]', '[STATE.3]')
  and ss_net_profit between 0 and 2000  
  )
 or
 (
  ca_state in ('[STATE.4]', '[STATE.5]', '[STATE.6]')
  and ss_net_profit between 150 and 3000 
  )
 or
  (
  ca_state in ('[STATE.7]', '[STATE.8]', '[STATE.9]')
  and ss_net_profit between 50 and 25000 
  )
 )
;

