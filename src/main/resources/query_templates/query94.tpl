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
[COMMENT] QUERY_ID=94;

define YEAR = random(1999, 2002, uniform);
define MONTH = random(2,5,uniform);
define STATE = dist(fips_county,3,1);   
define _LIMIT=100;

select  
   count(distinct ws_order_number) as col0
  ,sum(ws_ext_ship_cost)
  ,sum(ws_net_profit)
from
   web_sales ws1 join date_dim d
     on d.d_date between '[YEAR]-[MONTH]-01' and 
           date_add('[YEAR]-[MONTH]-01', 60)
     and ws1.ws_ship_date_sk = d.d_date_sk
   join customer_address ca
     on ws1.ws_ship_addr_sk = ca.ca_address_sk
     and ca.ca_state = '[STATE]'
   join web_site web
     on ws1.ws_web_site_sk = web.web_site_sk
     and web.web_company_name = 'pri'
   join (select ws_order_number as tmp_order_number, count(ws_warehouse_sk) as warehouse_cnt
            from web_sales ws2
            group by ws_order_number) tmp1
     on ws1.ws_order_number = tmp1.tmp_order_number
   left outer join (select distinct wr_order_number
               from web_returns wr1) tmp2
     on ws1.ws_order_number = tmp2.wr_order_number
where
    warehouse_cnt > 1 and wr_order_number is NULL
order by col0
[_LIMITC];

