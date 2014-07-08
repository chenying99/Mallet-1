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
[COMMENT] QUERY_ID=39;

define CATEGORY = text({"Books",1},{"Home",1},{"Electronics",1},{"Jewelry",1},{"Sports",1});
define MONTH = random(1,4,uniform);
Define YEAR = random(1998,2002, uniform);
define STATENUMBER=ulist(random(1, rowcount("active_states", "warehouse"), uniform),3);
define STATEA=distmember(fips_county,[STATENUMBER.1], 3);
define STATEB=distmember(fips_county,[STATENUMBER.2], 3);
define STATEC=distmember(fips_county,[STATENUMBER.3], 3);

-- drop temporary table
drop table if exists inv_[_STREAM];

-- create temporary table
create table inv_[_STREAM] (w_warehouse_name string, w_warehouse_sk bigint, i_item_sk bigint,
                  d_moy bigint, stdev double, mean double,cov double);

-- the query
insert overwrite table inv_[_STREAM]
select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy
       ,stdev,mean, case mean when 0.0 then null else stdev/mean end cov
 from(select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy
            ,stddev_samp(inv_quantity_on_hand) stdev,avg(inv_quantity_on_hand) mean
      from 
           inventory inv join item i
             on inv.inv_item_sk = i.i_item_sk
           join warehouse w
             on inv.inv_warehouse_sk = w.w_warehouse_sk
           join date_dim d
             on inv.inv_date_sk = d.d_date_sk
             and d.d_year =[YEAR]
      group by w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy) foo
 where case mean when 0.0 then 0.0 else stdev/mean end > 1;

select inv1.w_warehouse_sk,inv1.i_item_sk,inv1.d_moy,inv1.mean, inv1.cov
        ,inv2.w_warehouse_sk,inv2.i_item_sk,inv2.d_moy,inv2.mean, inv2.cov
from inv_[_STREAM] inv1 join inv_[_STREAM] inv2
  on inv1.i_item_sk = inv2.i_item_sk
  and inv1.w_warehouse_sk =  inv2.w_warehouse_sk
  and inv1.d_moy=[MONTH]
  and inv2.d_moy=[MONTH]+1
order by inv1.w_warehouse_sk,inv1.i_item_sk,inv1.d_moy,inv1.mean,inv1.cov
        ,inv2.d_moy,inv2.mean, inv2.cov
;


insert overwrite table inv_[_STREAM]
select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy
       ,stdev,mean, case mean when 0.0 then null else stdev/mean end cov
 from(select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy
            ,stddev_samp(inv_quantity_on_hand) stdev,avg(inv_quantity_on_hand) mean
      from 
           inventory inv join item i
             on inv.inv_item_sk = i.i_item_sk
           join warehouse w
             on inv.inv_warehouse_sk = w.w_warehouse_sk
           join date_dim d
             on inv.inv_date_sk = d.d_date_sk
             and d.d_year =[YEAR]
      group by w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy) foo
 where case mean when 0.0 then 0.0 else stdev/mean end > 1;

select inv1.w_warehouse_sk,inv1.i_item_sk,inv1.d_moy,inv1.mean, inv1.cov
        ,inv2.w_warehouse_sk,inv2.i_item_sk,inv2.d_moy,inv2.mean, inv2.cov
from inv_[_STREAM] inv1 join inv_[_STREAM] inv2
  on inv1.i_item_sk = inv2.i_item_sk
  and inv1.w_warehouse_sk =  inv2.w_warehouse_sk
  and inv1.d_moy=[MONTH]
  and inv2.d_moy=[MONTH]+1
  and inv1.cov > 1.5
order by inv1.w_warehouse_sk,inv1.i_item_sk,inv1.d_moy,inv1.mean,inv1.cov
        ,inv2.d_moy,inv2.mean, inv2.cov
;

-- drop temporary table
drop table inv_[_STREAM];
