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
[COMMENT] QUERY_ID=26;

 define GEN= dist(gender, 1, 1);
 define MS= dist(marital_status, 1, 1);
 define ES= dist(education, 1, 1);
 define YEAR = random(1998,2002,uniform);
 define _LIMIT=100; 

select  i_item_id, 
        avg(cs_quantity) agg1,
        avg(cs_list_price) agg2,
        avg(cs_coupon_amt) agg3,
        avg(cs_sales_price) agg4 
 from
    catalog_sales cs join customer_demographics cd
      on cs.cs_bill_cdemo_sk = cd.cd_demo_sk and cd.cd_gender = '[GEN]'
         and cd.cd_marital_status = '[MS]' and cd.cd_education_status = '[ES]'
    join date_dim d
      on cs.cs_sold_date_sk = d.d_date_sk and d.d_year = [YEAR] 
    join item i
      on cs.cs_item_sk = i.i_item_sk
    join promotion p
      on cs.cs_promo_sk = p.p_promo_sk
 where
    p_channel_email = 'N' or p_channel_event = 'N'
 group by i_item_id
 order by i_item_id
 [_LIMITC];
