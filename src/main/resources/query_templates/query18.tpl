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
[COMMENT] QUERY_ID=18;

 define YEAR = random(1998, 2002, uniform);
 define GEN= dist(gender, 1, 1);
 define ES= dist(education, 1, 1);
 define STATE=ulist(dist(fips_county,3,1),7);
 define MONTH=ulist(random(1,12,uniform),6);
 define _LIMIT=100;

select  i_item_id,
        ca_country,
        ca_state, 
        ca_county,
        avg( cast(cs_quantity as double)) agg1,
        avg( cast(cs_list_price as double)) agg2,
        avg( cast(cs_coupon_amt as double)) agg3,
        avg( cast(cs_sales_price as double)) agg4,
        avg( cast(cs_net_profit as double)) agg5,
        avg( cast(c_birth_year as double)) agg6,
        avg( cast(cd1.cd_dep_count as double)) agg7
 from 
      catalog_sales cs join customer_demographics cd1
        on cs.cs_bill_cdemo_sk = cd1.cd_demo_sk
        and cd1.cd_gender = '[GEN]'
        and cd1.cd_education_status = '[ES]'
      join customer c
        on cs.cs_bill_customer_sk = c.c_customer_sk
        and c.c_birth_month in ([MONTH.1],[MONTH.2],[MONTH.3],[MONTH.4],[MONTH.5],[MONTH.6])
      join customer_demographics cd2
        on c.c_current_cdemo_sk = cd2.cd_demo_sk
      join customer_address ca
        on c.c_current_addr_sk = ca.ca_address_sk
        and ca.ca_state in ('[STATE.1]','[STATE.2]','[STATE.3]'
                   ,'[STATE.4]','[STATE.5]','[STATE.6]','[STATE.7]')
      join date_dim d
        on cs.cs_sold_date_sk = d.d_date_sk
        and d.d_year = [YEAR]
      join item i
        on cs.cs_item_sk = i.i_item_sk
 group by i_item_id, ca_country, ca_state, ca_county with rollup
 order by ca_country,
        ca_state, 
        ca_county,
    i_item_id
 [_LIMITC];

