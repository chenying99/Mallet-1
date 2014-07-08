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
[COMMENT] QUERY_ID=6;

define YEAR = random(1998, 2002, uniform);
 define MONTH= random(1,7,uniform);
 define _LIMIT=100;

select  ca_state state, count(*) cnt
 from
     customer_address a join customer c
       on a.ca_address_sk = c.c_current_addr_sk
     join store_sales s
       on c.c_customer_sk = s.ss_customer_sk
     join date_dim d
       on s.ss_sold_date_sk = d.d_date_sk
     join item i
       on s.ss_item_sk = i.i_item_sk
     left semi join
        (select distinct (d_month_seq) as month_seq
         from date_dim
           where d_year = [YEAR]
            and d_moy = [MONTH] ) tmp2
        on d.d_month_seq=tmp2.month_seq
     join 
        (select i_category, avg(j.i_current_price) as avg_price
         from item j 
         group by i_category) tmp3
        on i.i_category = tmp3.i_category
 where i_current_price > 1.2 * avg_price
 group by ca_state
 having count(*) >= 10
 order by cnt 
 [_LIMITC];
