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
[COMMENT] QUERY_ID=24;

define MARKET=random(5,10,uniform);
define AMOUNTONE=text({"ss_net_paid",1},{"ss_net_paid_inc_tax",1},{"ss_net_profit",1},{"ss_sales_price",1},{"ss_ext_sales_price",1});
define COLOR=ulist(dist(colors,1,1),2);

-- drop temporary table
drop table if exists ssales_[_STREAM];

-- create temporary table
create table ssales_[_STREAM] (c_last_name string, c_first_name string, s_store_name string, ca_state string,
                     s_state string, i_color string, i_current_price double, i_manager_id bigint,
                     i_units string, i_size string, netpaid double);

-- the query
insert overwrite table ssales_[_STREAM]
select c_last_name
      ,c_first_name
      ,s_store_name
      ,ca_state
      ,s_state
      ,i_color
      ,i_current_price
      ,i_manager_id
      ,i_units
      ,i_size
      ,sum([AMOUNTONE]) netpaid
from 
    store_sales ss join store_returns sr
      on ss.ss_ticket_number = sr.sr_ticket_number
      and ss.ss_item_sk = sr.sr_item_sk
    join store s
      on ss.ss_store_sk = s.s_store_sk
      and s.s_market_id=[MARKET]
    join item i
      on ss.ss_item_sk = i.i_item_sk
    join customer c
      on ss.ss_customer_sk = c.c_customer_sk
    join customer_address ca
      on c.c_birth_country = upper(ca.ca_country)
      and s.s_zip = ca.ca_zip
group by c_last_name
        ,c_first_name
        ,s_store_name
        ,ca_state
        ,s_state
        ,i_color
        ,i_current_price
        ,i_manager_id
        ,i_units
        ,i_size;

select c_last_name
      ,c_first_name
      ,s_store_name
      ,paid
from (
    select c_last_name
          ,c_first_name
          ,s_store_name
          ,sum(netpaid) paid
    from ssales_[_STREAM]
    where i_color = '[COLOR.1]'
    group by c_last_name
            ,c_first_name
            ,s_store_name)
    tmp1 join (
    select 0.05*avg(netpaid) as avg_netpaid
           from ssales_[_STREAM]) tmp2
    where paid > avg_netpaid;


-- the query
insert overwrite table ssales_[_STREAM]
select c_last_name
      ,c_first_name
      ,s_store_name
      ,ca_state
      ,s_state
      ,i_color
      ,i_current_price
      ,i_manager_id
      ,i_units
      ,i_size
      ,sum([AMOUNTONE]) netpaid
from 
    store_sales ss join store_returns sr
      on ss.ss_ticket_number = sr.sr_ticket_number
      and ss.ss_item_sk = sr.sr_item_sk
    join store s
      on ss.ss_store_sk = s.s_store_sk
      and s.s_market_id = [MARKET]
    join item i
      on ss.ss_item_sk = i.i_item_sk
    join customer c
      on ss.ss_customer_sk = c.c_customer_sk
    join customer_address ca
      on c.c_birth_country = upper(ca.ca_country)
      and s.s_zip = ca.ca_zip
group by c_last_name
        ,c_first_name
        ,s_store_name
        ,ca_state
        ,s_state
        ,i_color
        ,i_current_price
        ,i_manager_id
        ,i_units
        ,i_size;

select c_last_name
      ,c_first_name
      ,s_store_name
      ,paid
from (
    select c_last_name
          ,c_first_name
          ,s_store_name
          ,sum(netpaid) paid
    from ssales_[_STREAM]
    where i_color = '[COLOR.2]'
    group by c_last_name
            ,c_first_name
            ,s_store_name)
    tmp1 join (
        select 0.05*avg(netpaid) as avg_netpaid
        from ssales_[_STREAM]) tmp2
    where paid > avg_netpaid;

-- drop temporary table
drop table ssales_[_STREAM];

