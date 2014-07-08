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
[COMMENT] QUERY_ID=80;

 define YEAR = random(1998, 2002, uniform);
 define SALES_DATE=date([YEAR]+"-08-01",[YEAR]+"-08-30",sales);
 define _LIMIT=100; 
 
 -- drop temporary table
drop table if exists ssr_[_STREAM];
drop table if exists csr_[_STREAM];
drop table if exists wsr_[_STREAM];

 -- create temporary table
create table ssr_[_STREAM] (store_id string, sales double, returns double, profit double);
create table csr_[_STREAM] (catalog_page_id string, sales double, returns double, profit double);
create table wsr_[_STREAM] (web_site_id string, sales double, returns double, profit double);

-- the query
insert overwrite table ssr_[_STREAM]
 select  s_store_id as store_id,
          sum(ss_ext_sales_price) as sales,
          sum(coalesce(sr_return_amt, 0)) as returns,
          sum(ss_net_profit - coalesce(sr_net_loss, 0)) as profit
  from store_sales ss left outer join store_returns sr on
         (ss.ss_item_sk = sr.sr_item_sk and ss.ss_ticket_number = sr.sr_ticket_number)
     join date_dim d
       on ss.ss_sold_date_sk = d.d_date_sk
       and d.d_date between '[SALES_DATE]'
                  and date_add('[SALES_DATE]', 30)
     join store s
       on ss.ss_store_sk = s.s_store_sk
     join item i
       on ss.ss_item_sk = i.i_item_sk
       and i.i_current_price > 50
     join promotion p
       on ss.ss_promo_sk = p.p_promo_sk
       and p.p_channel_tv = 'N'
 group by s_store_id;

insert overwrite table csr_[_STREAM]
 select  cp_catalog_page_id as catalog_page_id,
          sum(cs_ext_sales_price) as sales,
          sum(coalesce(cr_return_amount, 0)) as returns,
          sum(cs_net_profit - coalesce(cr_net_loss, 0)) as profit
  from catalog_sales cs left outer join catalog_returns cr on
         (cs.cs_item_sk = cr.cr_item_sk and cs.cs_order_number = cr.cr_order_number)
     join date_dim d
       on cs.cs_sold_date_sk = d.d_date_sk
       and d.d_date between '[SALES_DATE]'
                  and date_add('[SALES_DATE]', 30)
     join catalog_page cp
       on cs.cs_catalog_page_sk = cp.cp_catalog_page_sk
     join item i
       on cs.cs_item_sk = i.i_item_sk
       and i.i_current_price > 50
     join promotion p
       on cs.cs_promo_sk = p.p_promo_sk
       and p.p_channel_tv = 'N'
group by cp_catalog_page_id;

insert overwrite table wsr_[_STREAM]
 select  web_site_id,
          sum(ws_ext_sales_price) as sales,
          sum(coalesce(wr_return_amt, 0)) as returns,
          sum(ws_net_profit - coalesce(wr_net_loss, 0)) as profit
  from web_sales ws left outer join web_returns wr on
         (ws.ws_item_sk = wr.wr_item_sk and ws.ws_order_number = wr.wr_order_number)
     join date_dim d
       on ws.ws_sold_date_sk = d.d_date_sk
       and d.d_date between '[SALES_DATE]'
                  and date_add('[SALES_DATE]', 30)
     join web_site web
       on ws.ws_web_site_sk = web.web_site_sk
     join item i
       on ws.ws_item_sk = i.i_item_sk
       and i.i_current_price > 50
     join promotion p
       on ws.ws_promo_sk = p.p_promo_sk
       and p.p_channel_tv = 'N'
group by web_site_id;

  select  channel
        , id
        , sum(sales) as sales
        , sum(returns) as returns
        , sum(profit) as profit
 from 
 (select 'store channel' as channel
        , concat('store', store_id) as id
        , sales
        , returns
        , profit
 from   ssr_[_STREAM]
 union all
 select 'catalog channel' as channel
        , concat('catalog_page', catalog_page_id) as id
        , sales
        , returns
        , profit
 from  csr_[_STREAM]
 union all
 select 'web channel' as channel
        , concat('web_site', web_site_id) as id
        , sales
        , returns
        , profit
 from   wsr_[_STREAM]
 ) x
 group by channel, id with rollup
 order by channel
         ,id
 [_LIMITC];

-- drop temporary table
drop table ssr_[_STREAM];
drop table csr_[_STREAM];
drop table wsr_[_STREAM];

