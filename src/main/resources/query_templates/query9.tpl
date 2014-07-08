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
[COMMENT] QUERY_ID=9;

define AGGCTHEN= text({"ss_ext_discount_amt",1},{"ss_ext_sales_price",1},{"ss_ext_list_price",1},{"ss_ext_tax",1});
define AGGCELSE= text({"ss_net_paid",1},{"ss_net_paid_inc_tax",1},{"ss_net_profit",1});
define RC=ulist(random(1, rowcount("store_sales")/5,uniform),5);

select case when when1 > [RC.1]
            then then1
            else else1 end bucket1 ,
       case when when2 > [RC.2]
            then then2
            else else2 end bucket2,
       case when when3 > [RC.3]
            then then3
            else else3 end bucket3,
       case when when4 > [RC.4]
            then then4
            else else4 end bucket4,
       case when when5 > [RC.5]
            then then5
            else else5 end bucket5
from reason join (select count(*) as when1
                  from store_sales 
                  where ss_quantity between 1 and 20) tmp1_1
                on reason.r_reason_sk = 1
            join (select avg([AGGCTHEN]) as then1
                  from store_sales 
                  where ss_quantity between 1 and 20) tmp1_2
            join (select avg([AGGCELSE]) as else1
                  from store_sales
                  where ss_quantity between 1 and 20) tmp1_3
            join (select count(*) as when2
                  from store_sales
                  where ss_quantity between 21 and 40) tmp2_1
            join (select avg([AGGCTHEN]) as then2
                  from store_sales
                  where ss_quantity between 21 and 40) tmp2_2
            join (select avg([AGGCELSE]) as else2
                  from store_sales
                  where ss_quantity between 21 and 40) tmp2_3
            join (select count(*) as when3
                  from store_sales
                  where ss_quantity between 41 and 60) tmp3_1
            join (select avg([AGGCTHEN]) as then3
                  from store_sales
                  where ss_quantity between 41 and 60) tmp3_2
            join (select avg([AGGCELSE]) as else3
                  from store_sales
                  where ss_quantity between 41 and 60) tmp3_3
            join (select count(*) as when4
                  from store_sales
                  where ss_quantity between 61 and 80) tmp4_1
            join (select avg([AGGCTHEN]) as then4
                  from store_sales
                  where ss_quantity between 61 and 80) tmp4_2
            join (select avg([AGGCELSE]) as else4
                  from store_sales
                  where ss_quantity between 61 and 80) tmp4_3
            join (select count(*) as when5
                  from store_sales
                  where ss_quantity between 81 and 100) tmp5_1
            join (select avg([AGGCTHEN]) as then5
                  from store_sales
                  where ss_quantity between 81 and 100) tmp5_2
            join (select avg([AGGCELSE]) as else5
                  from store_sales
                  where ss_quantity between 81 and 100) tmp5_3
;

