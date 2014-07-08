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
[COMMENT] QUERY_ID=27;

define YEAR = random(1998, 2002, uniform);
 define GEN= dist(gender, 1, 1);
 define MS= dist(marital_status, 1, 1);
 define ES= dist(education, 1, 1);
 define STATENUMBER=ulist(random(1, rowcount("active_states", "store"), uniform),6);
 define STATE_A=distmember(fips_county,[STATENUMBER.1], 3);
 define STATE_B=distmember(fips_county,[STATENUMBER.2], 3);
 define STATE_C=distmember(fips_county,[STATENUMBER.3], 3);
 define STATE_D=distmember(fips_county,[STATENUMBER.4], 3);
 define STATE_E=distmember(fips_county,[STATENUMBER.5], 3);
 define STATE_F=distmember(fips_county,[STATENUMBER.6], 3);
 define _LIMIT=100;

select  i_item_id,
        s_state, 
        case when grouping__id >= 2 then 0 else 1 end,
        avg(ss_quantity) agg1,
        avg(ss_list_price) agg2,
        avg(ss_coupon_amt) agg3,
        avg(ss_sales_price) agg4
 from 
      store_sales ss join customer_demographics cd
        on ss.ss_cdemo_sk = cd.cd_demo_sk
        and cd.cd_gender = '[GEN]'
        and cd.cd_marital_status = '[MS]'
        and cd.cd_education_status = '[ES]'
      join date_dim d
        on ss.ss_sold_date_sk = d.d_date_sk
        and d.d_year = [YEAR]
      join store s
        on ss.ss_store_sk = s.s_store_sk
        and s.s_state in ('[STATE_A]','[STATE_B]', '[STATE_C]', '[STATE_D]', '[STATE_E]', '[STATE_F]')
      join item i
        on ss.ss_item_sk = i.i_item_sk
 group by i_item_id, s_state with rollup
 order by i_item_id
         ,s_state
 [_LIMITC];

