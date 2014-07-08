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
[COMMENT] QUERY_ID=44;

define NULLCOLSS= text({"ss_customer_sk",1},{"ss_cdemo_sk",1},{"ss_hdemo_sk",1},{"ss_addr_sk",1},{"ss_promo_sk",1});
define STORE=random(1,rowcount("STORE"),uniform);
define _LIMIT=100;


select  asceding.rnk, i1.i_product_name best_performing, i2.i_product_name worst_performing
from(select *
     from (select item_sk,rank() over (order by rank_col asc) rnk
           from (
                 select item_sk,rank_col
                 from (
                     select ss_item_sk item_sk,avg(ss_net_profit) rank_col 
                     from store_sales ss1
                     where ss_store_sk = [STORE]
                     group by ss_item_sk) tmp1
                   join 
                    (select avg(ss_net_profit) rank_col_2
                      from store_sales
                      where ss_store_sk = [STORE]
                        and [NULLCOLSS] is null
                      group by ss_store_sk) tmp2
                 where rank_col > 0.9*rank_col_2)V1)V11
     where rnk  < 11) asceding join
    (select *
     from (select item_sk,rank() over (order by rank_col desc) rnk
           from (
                 select item_sk,rank_col
                 from (
                     select ss_item_sk item_sk,avg(ss_net_profit) rank_col
                     from store_sales ss1
                     where ss_store_sk = [STORE]
                     group by ss_item_sk) tmp3
                  join
                    (select avg(ss_net_profit) rank_col_2
                      from store_sales
                      where ss_store_sk = [STORE]
                        and [NULLCOLSS] is null
                      group by ss_store_sk) tmp4
                 where rank_col > 0.9*rank_col_2)V2)V21
     where rnk  < 11) descending
       on asceding.rnk = descending.rnk 
join item i1
  on i1.i_item_sk=asceding.item_sk
join item i2
  on i2.i_item_sk=descending.item_sk
order by asceding.rnk
[_LIMITC];

