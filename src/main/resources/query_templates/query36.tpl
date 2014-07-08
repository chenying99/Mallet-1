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
[COMMENT] QUERY_ID=36;

 define YEAR=random(1998,2002,uniform);
 define STATENUMBER=ulist(random(1, rowcount("active_states", "store"), uniform),8);
 define STATE_A=distmember(fips_county,[STATENUMBER.1], 3);
 define STATE_B=distmember(fips_county,[STATENUMBER.2], 3);
 define STATE_C=distmember(fips_county,[STATENUMBER.3], 3);
 define STATE_D=distmember(fips_county,[STATENUMBER.4], 3);
 define STATE_E=distmember(fips_county,[STATENUMBER.5], 3);
 define STATE_F=distmember(fips_county,[STATENUMBER.6], 3);
 define STATE_G=distmember(fips_county,[STATENUMBER.7], 3);
 define STATE_H=distmember(fips_county,[STATENUMBER.8], 3);
 define _LIMIT=100;
 
 select
    gross_margin,i_category,i_class,
    grouping_i_category+grouping_i_class as lochierarchy,
    rank() over (
    partition by grouping_i_category+grouping_i_class,
    case when grouping_i_class = 0 then i_category end 
    order by gross_margin asc) as rank_within_parent
from (  
    select  
        sum(ss_net_profit)/sum(ss_ext_sales_price) as gross_margin
       ,i_category
       ,i_class
       ,case when grouping__id = 1 or grouping__id = 3 then 0 else 1
        end as grouping_i_category
       ,case when grouping__id >= 2 then 0 else 1 end as grouping_i_class
     from
        store_sales ss join date_dim       d1
          on d1.d_year = [YEAR] 
          and d1.d_date_sk = ss.ss_sold_date_sk
        join item i
          on i.i_item_sk  = ss.ss_item_sk 
        join store s
          on s.s_store_sk  = ss.ss_store_sk
          and s.s_state in ('[STATE_A]','[STATE_B]','[STATE_C]','[STATE_D]',
                 '[STATE_E]','[STATE_F]','[STATE_G]','[STATE_H]')
     group by i_category,i_class with rollup) x
 order by
   lochierarchy desc
  ,case when lochierarchy = 0 then i_category end
  ,rank_within_parent
  [_LIMITC];

