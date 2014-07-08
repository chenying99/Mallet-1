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
[COMMENT] QUERY_ID=78;

define YEAR=random(1998, 2002, uniform);
define SELECTCONE=text({"ss_sold_year",1},{"ss_item_sk",1},{"ss_customer_sk",1},{"ss_sold_year, ss_item_sk, ss_customer_sk",1});

define _LIMIT = 100;

-- drop temporary table
drop table if exists ws_[_STREAM];
drop table if exists cs_[_STREAM];
drop table if exists ss_[_STREAM];

-- create temporary table
create table ws_[_STREAM] (ws_sold_year bigint, ws_item_sk bigint, ws_customer_sk bigint, ws_qty bigint,
                 ws_wc double, ws_sp double);
create table cs_[_STREAM] (cs_sold_year bigint, cs_item_sk bigint, cs_customer_sk bigint, cs_qty bigint,
                 cs_wc double, cs_sp double);
create table ss_[_STREAM] (ss_sold_year bigint, ss_item_sk bigint, ss_customer_sk bigint, ss_qty bigint,
                 ss_wc double, ss_sp double);

-- the query
insert overwrite table ws_[_STREAM]
   select d_year AS ws_sold_year, ws_item_sk,
    ws_bill_customer_sk ws_customer_sk,
    sum(ws_quantity) ws_qty,
    sum(ws_wholesale_cost) ws_wc,
    sum(ws_sales_price) ws_sp
   from web_sales ws
   left outer join web_returns wr on wr.wr_order_number=ws.ws_order_number and ws.ws_item_sk=wr.wr_item_sk
   join date_dim d on ws.ws_sold_date_sk = d.d_date_sk
   where wr_order_number is null
   group by d_year, ws_item_sk, ws_bill_customer_sk;

insert overwrite table cs_[_STREAM]
   select d_year AS cs_sold_year, cs_item_sk,
    cs_bill_customer_sk cs_customer_sk,
    sum(cs_quantity) cs_qty,
    sum(cs_wholesale_cost) cs_wc,
    sum(cs_sales_price) cs_sp
   from catalog_sales cs
   left outer join catalog_returns cr on cr.cr_order_number=cs.cs_order_number and cs.cs_item_sk=cr.cr_item_sk
   join date_dim d on cs.cs_sold_date_sk = d.d_date_sk
   where cr_order_number is null
   group by d_year, cs_item_sk, cs_bill_customer_sk;

insert overwrite table ss_[_STREAM]
   select d_year AS ss_sold_year, ss_item_sk,
    ss_customer_sk,
    sum(ss_quantity) ss_qty,
    sum(ss_wholesale_cost) ss_wc,
    sum(ss_sales_price) ss_sp
   from store_sales ss
   left outer join store_returns sr on sr.sr_ticket_number=ss.ss_ticket_number and ss.ss_item_sk=sr.sr_item_sk
   join date_dim d on ss.ss_sold_date_sk = d.d_date_sk
   where sr_ticket_number is null
   group by d_year, ss_item_sk, ss_customer_sk;

 select 
[SELECTCONE],
round(ss_qty/(coalesce(ws_qty+cs_qty,1)),2) ratio,
ss_qty store_qty, ss_wc store_wholesale_cost, ss_sp store_sales_price,
coalesce(ws_qty,0)+coalesce(cs_qty,0) other_chan_qty,
coalesce(ws_wc,0)+coalesce(cs_wc,0) other_chan_wholesale_cost,
coalesce(ws_sp,0)+coalesce(cs_sp,0) other_chan_sales_price
from ss_[_STREAM]
left outer join ws_[_STREAM] on (ws_[_STREAM].ws_sold_year=ss_[_STREAM].ss_sold_year and ws_[_STREAM].ws_item_sk=ss_[_STREAM].ss_item_sk and ws_[_STREAM].ws_customer_sk=ss_[_STREAM].ss_customer_sk)
left outer join cs_[_STREAM] on (cs_[_STREAM].cs_sold_year=ss_[_STREAM].ss_sold_year and cs_[_STREAM].cs_item_sk=ss_[_STREAM].ss_item_sk and cs_[_STREAM].cs_customer_sk=ss_[_STREAM].ss_customer_sk)
where coalesce(ws_qty,0)>0 and coalesce(cs_qty, 0)>0 and ss_sold_year=[YEAR]
order by 
  [SELECTCONE],
  store_qty desc, store_wholesale_cost desc, store_sales_price desc,
  other_chan_qty,
  other_chan_wholesale_cost,
  other_chan_sales_price,
  ratio
[_LIMITC];

-- drop temporary table
drop table ws_[_STREAM];
drop table cs_[_STREAM];
drop table ss_[_STREAM];
