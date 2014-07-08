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
[COMMENT] QUERY_ID=77;

 define YEAR = random(1998, 2002, uniform);
 define SALES_DATE=date([YEAR]+"-08-01",[YEAR]+"-08-30",sales);
 define _LIMIT=100;

-- drop temporary table
drop table if exists ss_[_STREAM];
drop table if exists sr_[_STREAM];
drop table if exists cs_[_STREAM];
drop table if exists cr_[_STREAM];
drop table if exists ws_[_STREAM];
drop table if exists wr_[_STREAM];
 
-- create temporary table
create table ss_[_STREAM] (s_store_sk bigint, sales double, profit double);
create table sr_[_STREAM] (s_store_sk bigint, returns double, profit_loss double);
create table cs_[_STREAM] (cs_call_center_sk bigint, sales double, profit double);
create table cr_[_STREAM] (returns double, profit_loss double);
create table ws_[_STREAM] (wp_web_page_sk bigint, sales double, profit double);
create table wr_[_STREAM] (wp_web_page_sk bigint, returns double, profit_loss double);

-- the query
insert overwrite table ss_[_STREAM]
 select s_store_sk,
         sum(ss_ext_sales_price) as sales,
         sum(ss_net_profit) as profit
 from 
      store_sales ss join date_dim d
        on ss.ss_sold_date_sk = d.d_date_sk
        and d.d_date between '[SALES_DATE]'
                  and date_add('[SALES_DATE]', 30) 
      join store s
        on ss.ss_store_sk = s.s_store_sk
 group by s_store_sk;

insert overwrite table sr_[_STREAM]
 select s_store_sk,
         sum(sr_return_amt) as returns,
         sum(sr_net_loss) as profit_loss
 from 
      store_returns sr join date_dim d
        on sr.sr_returned_date_sk = d.d_date_sk
        and d.d_date between '[SALES_DATE]'
                  and date_add('[SALES_DATE]', 30)
      join store s
        on sr.sr_store_sk = s.s_store_sk
 group by s_store_sk;

insert overwrite table cs_[_STREAM]
 select cs_call_center_sk,
        sum(cs_ext_sales_price) as sales,
        sum(cs_net_profit) as profit
 from 
      catalog_sales cs join date_dim d
        on cs.cs_sold_date_sk = d.d_date_sk
        and d.d_date between '[SALES_DATE]'
                  and date_add('[SALES_DATE]', 30)
 group by cs_call_center_sk;

insert overwrite table cr_[_STREAM]
 select
        sum(cr_return_amount) as returns,
        sum(cr_net_loss) as profit_loss
 from catalog_returns cr join date_dim d
     on cr.cr_returned_date_sk = d.d_date_sk
     and d.d_date between '[SALES_DATE]'
                  and date_add('[SALES_DATE]', 30);

insert overwrite table ws_[_STREAM]
 select wp_web_page_sk,
        sum(ws_ext_sales_price) as sales,
        sum(ws_net_profit) as profit
 from 
      web_sales ws join date_dim d
        on ws.ws_sold_date_sk = d.d_date_sk
        and d.d_date between '[SALES_DATE]'
                  and date_add('[SALES_DATE]', 30)
      join web_page wp
        on ws.ws_web_page_sk = wp.wp_web_page_sk
 group by wp_web_page_sk;

insert overwrite table wr_[_STREAM]
 select wp_web_page_sk,
        sum(wr_return_amt) as returns,
        sum(wr_net_loss) as profit_loss
 from 
      web_returns wr join date_dim d
        on wr.wr_returned_date_sk = d.d_date_sk
        and d.d_date between '[SALES_DATE]'
                  and date_add('[SALES_DATE]', 30)
      join web_page wp
        on wr.wr_web_page_sk = wp.wp_web_page_sk
 group by wp_web_page_sk;

  select  channel
        , id
        , sum(sales) as sales
        , sum(returns) as returns
        , sum(profit) as profit
 from 
 (select 'store channel' as channel
        , ss_[_STREAM].s_store_sk as id
        , sales
        , coalesce(returns, 0) as returns
        , (profit - coalesce(profit_loss,0)) as profit
 from   ss_[_STREAM] left outer join sr_[_STREAM]
        on  ss_[_STREAM].s_store_sk = sr_[_STREAM].s_store_sk
 union all
 select 'catalog channel' as channel
        , cs_call_center_sk as id
        , sales
        , returns
        , (profit - profit_loss) as profit
 from  cs_[_STREAM] join
       cr_[_STREAM]
 union all
 select 'web channel' as channel
        , ws_[_STREAM].wp_web_page_sk as id
        , sales
        , coalesce(returns, 0) returns
        , (profit - coalesce(profit_loss,0)) as profit
 from   ws_[_STREAM] left outer join wr_[_STREAM]
        on  ws_[_STREAM].wp_web_page_sk = wr_[_STREAM].wp_web_page_sk
 ) x
 group by channel, id with rollup
 order by channel
         ,id
 [_LIMITC];

-- drop temporary table
drop table ss_[_STREAM];
drop table sr_[_STREAM];
drop table cs_[_STREAM];
drop table cr_[_STREAM];
drop table ws_[_STREAM];
drop table wr_[_STREAM];
 

