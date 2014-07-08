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
[COMMENT] QUERY_ID=19;

define YEAR= random(1998, 2002, uniform);
 define MONTH=random(11,12,uniform);
 define MGR_IDX = dist(i_manager_id, 1, 1);
 define MANAGER=random(distmember(i_manager_id, [MGR_IDX], 2), distmember(i_manager_id, [MGR_IDX], 3),uniform);
 define _LIMIT=100;

select  i_brand_id brand_id, i_brand brand, i_manufact_id, i_manufact,
    sum(ss_ext_sales_price) ext_price
 from
    date_dim d join store_sales ss
      on d.d_date_sk = ss.ss_sold_date_sk and d.d_moy=[MONTH] and d.d_year=[YEAR]
    join item i
      on ss.ss_item_sk = i.i_item_sk and i.i_manager_id=[MANAGER]
    join customer c
      on ss.ss_customer_sk = c.c_customer_sk 
    join customer_address ca
      on c.c_current_addr_sk = ca.ca_address_sk
    join store s
      on ss.ss_store_sk = s.s_store_sk
 where 
   substr(ca_zip,1,5) <> substr(s_zip,1,5) 
 group by i_brand
      ,i_brand_id
      ,i_manufact_id
      ,i_manufact
 order by ext_price desc
         ,brand
         ,brand_id
         ,i_manufact_id
         ,i_manufact
[_LIMITC];
