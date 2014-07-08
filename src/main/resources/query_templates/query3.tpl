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
[COMMENT] QUERY_ID=3;

define AGGC= text({"ss_ext_sales_price",1},{"ss_sales_price",1},{"ss_ext_discount_amt",1},{"ss_net_profit",1});
 define MONTH = random(11,12,uniform);
 define MANUFACT= random(1,1000,uniform);
 define _LIMIT=100;

select  dt.d_year 
       ,item.i_brand_id brand_id 
       ,item.i_brand brand
       ,sum([AGGC]) sum_agg
 from
      date_dim dt join store_sales
        on dt.d_date_sk = store_sales.ss_sold_date_sk
        and dt.d_moy=[MONTH]
      join item
        on store_sales.ss_item_sk = item.i_item_sk
        and item.i_manufact_id = [MANUFACT]
 group by dt.d_year
      ,item.i_brand
      ,item.i_brand_id
 order by d_year
         ,sum_agg desc
         ,brand_id
 [_LIMITC];
