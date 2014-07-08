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
[COMMENT] QUERY_ID=33;

define YEAR= random(1998,2002, uniform);
define MONTH = random(1,7,uniform);
define COUNTY=random(1, rowcount("active_counties", "store"), uniform);
define GMT=distmember(fips_county,[COUNTY], 6);
define CATEGORY = text({"Books",1},{"Home",1},{"Electronics",1},{"Jewelry",1},{"Sports",1});
define _LIMIT=100;
 
-- drop temporary table
drop table if exists ss_[_STREAM];
drop table if exists cs_[_STREAM];
drop table if exists ws_[_STREAM];

-- create temporary table
create table ss_[_STREAM] (i_manufact_id bigint, total_sales double);
create table cs_[_STREAM] (i_manufact_id bigint, total_sales double);
create table ws_[_STREAM] (i_manufact_id bigint, total_sales double);

-- the query
insert overwrite table ss_[_STREAM]
 select
          i_manufact_id,sum(ss_ext_sales_price) total_sales
 from (
     select
              i_manufact_id,ss_ext_sales_price
     from
        store_sales ss join date_dim d
          on ss.ss_sold_date_sk         = d.d_date_sk
          and d.d_year                  = [YEAR]
          and d.d_moy                   = [MONTH]
        join customer_address ca
          on ss.ss_addr_sk              = ca.ca_address_sk
          and ca.ca_gmt_offset          = [GMT]
        join item i
          on ss.ss_item_sk              = i.i_item_sk)
    tmp1 left semi join (
        select
          i_manufact_id
        from
         item
        where i_category in ('[CATEGORY]')) tmp2
    on tmp1.i_manufact_id = tmp2.i_manufact_id
    group by i_manufact_id;

insert overwrite table cs_[_STREAM]
 select
          i_manufact_id,sum(cs_ext_sales_price) total_sales
 from (
     select
              i_manufact_id,cs_ext_sales_price
     from
        catalog_sales cs join date_dim d
          on cs.cs_sold_date_sk         = d.d_date_sk
          and d.d_year                  = [YEAR]
          and d.d_moy                   = [MONTH]
        join customer_address ca
          on cs.cs_bill_addr_sk         = ca.ca_address_sk
          and ca.ca_gmt_offset          = [GMT]
        join item i
          on cs.cs_item_sk              = i.i_item_sk )
    tmp1 left semi join (
        select
          i_manufact_id
        from
         item
        where i_category in ('[CATEGORY]')) tmp2
    on tmp1.i_manufact_id = tmp2.i_manufact_id
    group by i_manufact_id;

insert overwrite table ws_[_STREAM]
 select
          i_manufact_id,sum(ws_ext_sales_price) total_sales
 from (
     select
              i_manufact_id,ws_ext_sales_price
     from
        web_sales ws join date_dim d
          on ws.ws_sold_date_sk         = d.d_date_sk
          and d.d_year                  = [YEAR]
          and d.d_moy                   = [MONTH]
        join customer_address ca
          on ws.ws_bill_addr_sk         = ca.ca_address_sk
          and ca.ca_gmt_offset          = [GMT]
        join item i
          on ws.ws_item_sk              = i.i_item_sk)
    tmp1 left semi join (
        select
          i_manufact_id
        from
         item
        where i_category in ('[CATEGORY]')) tmp2
    on tmp1.i_manufact_id = tmp2.i_manufact_id
    group by i_manufact_id;


  select  i_manufact_id ,sum(total_sales) total_sales
 from  (select * from ss_[_STREAM]
        union all
        select * from cs_[_STREAM] 
        union all
        select * from ws_[_STREAM]) tmp1
 group by i_manufact_id
 order by total_sales
 [_LIMITC];

-- drop temporary table
drop table ss_[_STREAM];
drop table cs_[_STREAM];
drop table ws_[_STREAM];

