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
[COMMENT] QUERY_ID=5;

define YEAR = random(1998, 2002, uniform);
 define SALES_DATE=date([YEAR]+"-08-01",[YEAR]+"-08-30",sales);
 define _LIMIT=100;

-- drop temporary table
drop table if exists ssr_[_STREAM];
drop table if exists csr_[_STREAM];
drop table if exists wsr_[_STREAM];

-- create temporary table
create table ssr_[_STREAM] (s_store_id string, sales double, profit double,
                  returns double, profit_loss double);
create table csr_[_STREAM] (cp_catalog_page_id string, sales double, profit double,
                  returns double, profit_loss double);
create table wsr_[_STREAM] (web_site_id string, sales double, profit double,
                  returns double, profit_loss double);
-- the query
insert overwrite table ssr_[_STREAM]
 select s_store_id,
        sum(sales_price) as sales,
        sum(profit) as profit,
        sum(return_amt) as returns,
        sum(net_loss) as profit_loss
 from
  ( select  ss_store_sk as store_sk,
            ss_sold_date_sk  as date_sk,
            ss_ext_sales_price as sales_price,
            ss_net_profit as profit,
            cast(0 as double) as return_amt,
            cast(0 as double) as net_loss
    from store_sales
    union all
    select sr_store_sk as store_sk,
           sr_returned_date_sk as date_sk,
           cast(0 as double) as sales_price,
           cast(0 as double) as profit,
           sr_return_amt as return_amt,
           sr_net_loss as net_loss
    from store_returns
   ) salesreturns join date_dim d
       on salesreturns.date_sk = d.d_date_sk
       and d.d_date between '[SALES_DATE]'
                  and date_add('[SALES_DATE]', 14)
     join store s
       on salesreturns.store_sk = s.s_store_sk
 group by s_store_id;
 
insert overwrite table csr_[_STREAM]
 select cp_catalog_page_id,
        sum(sales_price) as sales,
        sum(profit) as profit,
        sum(return_amt) as returns,
        sum(net_loss) as profit_loss
 from
  ( select  cs_catalog_page_sk as page_sk,
            cs_sold_date_sk  as date_sk,
            cs_ext_sales_price as sales_price,
            cs_net_profit as profit,
            cast(0 as double) as return_amt,
            cast(0 as double) as net_loss
    from catalog_sales
    union all
    select cr_catalog_page_sk as page_sk,
           cr_returned_date_sk as date_sk,
           cast(0 as double) as sales_price,
           cast(0 as double) as profit,
           cr_return_amount as return_amt,
           cr_net_loss as net_loss
    from catalog_returns
   ) salesreturns join date_dim d
       on salesreturns.date_sk = d.d_date_sk
       and d.d_date between '[SALES_DATE]'
                  and date_add('[SALES_DATE]', 14)
     join catalog_page cp
       on salesreturns.page_sk = cp.cp_catalog_page_sk
 group by cp_catalog_page_id;

insert overwrite table wsr_[_STREAM]
 select web_site_id,
        sum(sales_price) as sales,
        sum(profit) as profit,
        sum(return_amt) as returns,
        sum(net_loss) as profit_loss
 from
  ( select  ws_web_site_sk as wsr_web_site_sk,
            ws_sold_date_sk  as date_sk,
            ws_ext_sales_price as sales_price,
            ws_net_profit as profit,
            cast(0 as double) as return_amt,
            cast(0 as double) as net_loss
    from web_sales
    union all
    select ws_web_site_sk as wsr_web_site_sk,
           wr_returned_date_sk as date_sk,
           cast(0 as double) as sales_price,
           cast(0 as double) as profit,
           wr_return_amt as return_amt,
           wr_net_loss as net_loss
    from web_returns wr left outer join web_sales ws on
         ( wr.wr_item_sk = ws.ws_item_sk
           and wr.wr_order_number = ws.ws_order_number)
   ) salesreturns join date_dim d
       on salesreturns.date_sk = d.d_date_sk
       and d.d_date between '[SALES_DATE]'
                  and date_add('[SALES_DATE]', 14)
     join web_site web
       on salesreturns.wsr_web_site_sk = web.web_site_sk
 group by web_site_id;


  select  channel
        , id
        , sum(sales) as sales
        , sum(returns) as returns
        , sum(profit) as profit
 from 
 (select 'store channel' as channel
        , concat('store', s_store_id) as id
        , sales
        , returns
        , (profit - profit_loss) as profit
 from   ssr_[_STREAM]
 union all
 select 'catalog channel' as channel
        , concat('catalog_page', cp_catalog_page_id) as id
        , sales
        , returns
        , (profit - profit_loss) as profit
 from  csr_[_STREAM]
 union all
 select 'web channel' as channel
        , concat('web_site', web_site_id) as id
        , sales
        , returns
        , (profit - profit_loss) as profit
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

