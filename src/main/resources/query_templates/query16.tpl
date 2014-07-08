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
[COMMENT] QUERY_ID=16;

define YEAR = random(1999, 2002, uniform);
define MONTH = random(2,5,uniform);
define STATE = dist(fips_county,3,1);   
define COUNTYNUMBER = ulist(random(1, rowcount("active_counties", "call_center"), uniform), 5);
define COUNTY_A = distmember(fips_county, [COUNTYNUMBER.1], 2);
define COUNTY_B = distmember(fips_county, [COUNTYNUMBER.2], 2);
define COUNTY_C = distmember(fips_county, [COUNTYNUMBER.3], 2);
define COUNTY_D = distmember(fips_county, [COUNTYNUMBER.4], 2);
define COUNTY_E = distmember(fips_county, [COUNTYNUMBER.5], 2);
define _LIMIT=100;

select  
   count(distinct cs_order_number) as col0
  ,sum(cs_ext_ship_cost)
  ,sum(cs_net_profit)
from
   catalog_sales cs1 join date_dim d
     on d.d_date between '[YEAR]-[MONTH]-01' and 
           date_add('[YEAR]-[MONTH]-01', 60)
     and cs1.cs_ship_date_sk = d.d_date_sk
   join customer_address ca
     on cs1.cs_ship_addr_sk = ca.ca_address_sk
     and ca.ca_state = '[STATE]'
   join call_center cc
     on cs1.cs_call_center_sk = cc.cc_call_center_sk
     and cc.cc_county in ('[COUNTY_A]','[COUNTY_B]','[COUNTY_C]','[COUNTY_D]',
                  '[COUNTY_E]')
   join (select cs_order_number as tmp_order_number, count(cs_warehouse_sk) as warehouse_cnt
            from catalog_sales cs2
            group by cs_order_number) tmp1
      on cs1.cs_order_number = tmp1.tmp_order_number
   left outer join (select distinct cr_order_number
               from catalog_returns cr1) tmp2
      on cs1.cs_order_number = tmp2.cr_order_number
where
    warehouse_cnt > 1 and cr_order_number is NULL
order by col0
[_LIMITC];

