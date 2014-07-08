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
[COMMENT] QUERY_ID=64;

define COLOR=ulist(dist(colors,1,1),6);
define PRICE=random(0,85,uniform);
define YEAR = random(1999, 2001, uniform);

-- drop temporary table
drop table if exists cs_ui_[_STREAM];
drop table if exists cross_sales_[_STREAM];

-- create temporary table
create table cs_ui_[_STREAM] (cs_item_sk bigint, sale double, refund double);
create table cross_sales_[_STREAM] (product_name string, item_sk bigint, store_name string, store_zip string,
                          b_street_number string, b_streen_name string, b_city string, b_zip string,
                          c_street_number string, c_street_name string, c_city string, c_zip string,
                          syear bigint, fsyear bigint, s2year bigint, cnt bigint, s1 double, s2 double,
                          s3 double);

-- the query
insert overwrite table cs_ui_[_STREAM]
  select cs_item_sk
        ,sum(cs_ext_list_price) as sale,sum(cr_refunded_cash+cr_reversed_charge+cr_store_credit) as refund
  from catalog_sales cs join catalog_returns cr
    on cs.cs_item_sk = cr.cr_item_sk
    and cs.cs_order_number = cr.cr_order_number
  group by cs_item_sk
  having sum(cs_ext_list_price)>2*sum(cr_refunded_cash+cr_reversed_charge+cr_store_credit);

insert overwrite table cross_sales_[_STREAM]
  select i_product_name product_name
     ,i_item_sk item_sk
     ,s_store_name store_name
     ,s_zip store_zip
     ,ad1.ca_street_number b_street_number
     ,ad1.ca_street_name b_streen_name
     ,ad1.ca_city b_city
     ,ad1.ca_zip b_zip
     ,ad2.ca_street_number c_street_number
     ,ad2.ca_street_name c_street_name
     ,ad2.ca_city c_city
     ,ad2.ca_zip c_zip
     ,d1.d_year as syear
     ,d2.d_year as fsyear
     ,d3.d_year s2year
     ,count(*) cnt
     ,sum(ss_wholesale_cost) s1
     ,sum(ss_list_price) s2
     ,sum(ss_coupon_amt) s3
  FROM   
        store_sales ss join store_returns sr
          on ss.ss_item_sk = sr.sr_item_sk
          and ss.ss_ticket_number = sr.sr_ticket_number
        join cs_ui_[_STREAM]
          on ss.ss_item_sk = cs_ui_[_STREAM].cs_item_sk
        join date_dim d1
          on ss.ss_sold_date_sk = d1.d_date_sk
        join store s
          on ss.ss_store_sk = s.s_store_sk
        join customer c
          on ss.ss_customer_sk = c.c_customer_sk
        join date_dim d2
          on c.c_first_sales_date_sk = d2.d_date_sk
        join date_dim d3
          on c.c_first_shipto_date_sk = d3.d_date_sk
        join customer_demographics cd1
          on ss.ss_cdemo_sk= cd1.cd_demo_sk
        join customer_demographics cd2
          on c.c_current_cdemo_sk = cd2.cd_demo_sk
        join promotion p
          on ss.ss_promo_sk = p.p_promo_sk
        join household_demographics hd1
          on ss.ss_hdemo_sk = hd1.hd_demo_sk
        join household_demographics hd2
          on c.c_current_hdemo_sk = hd2.hd_demo_sk
        join customer_address ad1
          on ss.ss_addr_sk = ad1.ca_address_sk
        join customer_address ad2
          on c.c_current_addr_sk = ad2.ca_address_sk
        join income_band ib1
          on hd1.hd_income_band_sk = ib1.ib_income_band_sk
        join income_band ib2
          on hd2.hd_income_band_sk = ib2.ib_income_band_sk
        join item i
          on ss.ss_item_sk = i.i_item_sk
          and i.i_color in ('[COLOR.1]','[COLOR.2]','[COLOR.3]','[COLOR.4]','[COLOR.5]','[COLOR.6]')
          and i.i_current_price between [PRICE] and [PRICE] + 10
          and i.i_current_price between [PRICE] + 1 and [PRICE] + 15
  WHERE  
         cd1.cd_marital_status <> cd2.cd_marital_status
group by i_product_name
       ,i_item_sk
       ,s_store_name
       ,s_zip
       ,ad1.ca_street_number
       ,ad1.ca_street_name
       ,ad1.ca_city
       ,ad1.ca_zip
       ,ad2.ca_street_number
       ,ad2.ca_street_name
       ,ad2.ca_city
       ,ad2.ca_zip
       ,d1.d_year
       ,d2.d_year
       ,d3.d_year;

select cs1.product_name
     ,cs1.store_name
     ,cs1.store_zip
     ,cs1.b_street_number
     ,cs1.b_streen_name
     ,cs1.b_city
     ,cs1.b_zip
     ,cs1.c_street_number
     ,cs1.c_street_name
     ,cs1.c_city
     ,cs1.c_zip
     ,cs1.syear
     ,cs1.cnt
     ,cs1.s1
     ,cs1.s2
     ,cs1.s3
     ,cs2.s1
     ,cs2.s2
     ,cs2.s3
     ,cs2.syear
     ,cs2.cnt
from cross_sales_[_STREAM] cs1 join cross_sales_[_STREAM] cs2
  on cs1.item_sk=cs2.item_sk and
     cs1.syear = [YEAR] and
     cs2.syear = [YEAR] + 1 and
     cs1.store_name = cs2.store_name and
     cs1.store_zip = cs2.store_zip
where
     cs2.cnt <= cs1.cnt
order by cs1.product_name
       ,cs1.store_name
       ,cs2.cnt;

-- drop temporary table
drop table cs_ui_[_STREAM];
drop table cross_sales_[_STREAM];

