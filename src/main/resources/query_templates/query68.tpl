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
[COMMENT] QUERY_ID=68;

 define DEPCNT=random(0,9,uniform);
 define YEAR = random(1998,2000,uniform);
 define VEHCNT=random(-1,4,uniform);
 define CITYNUMBER = ulist(random(1, rowcount("active_cities", "store"), uniform), 2);
 define CITY_A = distmember(cities, [CITYNUMBER.1], 1);
 define CITY_B = distmember(cities, [CITYNUMBER.2], 1);
 define _LIMIT=100;
 
 select  c_last_name
       ,c_first_name
       ,ca_city
       ,bought_city
       ,ss_ticket_number
       ,extended_price
       ,extended_tax
       ,list_price
 from (select ss_ticket_number
             ,ss_customer_sk
             ,ca_city bought_city
             ,sum(ss_ext_sales_price) extended_price 
             ,sum(ss_ext_list_price) list_price
             ,sum(ss_ext_tax) extended_tax 
       from
            store_sales join date_dim
              on store_sales.ss_sold_date_sk = date_dim.d_date_sk
              and date_dim.d_dom between 1 and 2 
              and date_dim.d_year in ([YEAR],[YEAR]+1,[YEAR]+2)
            join store
              on store_sales.ss_store_sk = store.s_store_sk
              and store.s_city in ('[CITY_A]','[CITY_B]')
            join household_demographics
              on store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
            join customer_address
              on store_sales.ss_addr_sk = customer_address.ca_address_sk
       where
            (household_demographics.hd_dep_count = [DEPCNT] or
             household_demographics.hd_vehicle_count= [VEHCNT])
       group by ss_ticket_number
               ,ss_customer_sk
               ,ss_addr_sk,ca_city)
    dn join customer
      on dn.ss_customer_sk = customer.c_customer_sk
    join customer_address current_addr
      on customer.c_current_addr_sk = current_addr.ca_address_sk
 where
    current_addr.ca_city <> bought_city
 order by c_last_name
         ,ss_ticket_number
 [_LIMITC];

