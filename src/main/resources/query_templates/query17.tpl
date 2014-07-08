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
[COMMENT] QUERY_ID=17;

define YEAR= random(1998,2002, uniform);
 define QRT = random(1,4,uniform); 
 define _LIMIT=100;

select  i_item_id
       ,i_item_desc
       ,s_state
       ,count(ss_quantity) as store_sales_quantitycount
       ,avg(ss_quantity) as store_sales_quantityave
       ,stddev_samp(ss_quantity) as store_sales_quantitystdev
       ,stddev_samp(ss_quantity)/avg(ss_quantity) as store_sales_quantitycov
       ,count(sr_return_quantity) as_store_returns_quantitycount
       ,avg(sr_return_quantity) as_store_returns_quantityave
       ,stddev_samp(sr_return_quantity) as_store_returns_quantitystdev
       ,stddev_samp(sr_return_quantity)/avg(sr_return_quantity) as store_returns_quantitycov
       ,count(cs_quantity) as catalog_sales_quantitycount ,avg(cs_quantity) as catalog_sales_quantityave
       ,stddev_samp(cs_quantity)/avg(cs_quantity) as catalog_sales_quantitystdev
       ,stddev_samp(cs_quantity)/avg(cs_quantity) as catalog_sales_quantitycov
 from 
     store_sales ss join store_returns sr
       on ss.ss_customer_sk = sr.sr_customer_sk and ss.ss_item_sk = sr.sr_item_sk and ss.ss_ticket_number = sr.sr_ticket_number
     join catalog_sales cs
       on sr.sr_customer_sk = cs.cs_bill_customer_sk and sr.sr_item_sk = cs.cs_item_sk
     join date_dim d1
       on d1.d_quarter_name = '[YEAR]Q1' and d1.d_date_sk = ss.ss_sold_date_sk
     join date_dim d2
       on sr.sr_returned_date_sk = d2.d_date_sk and d2.d_quarter_name in ('[YEAR]Q1','[YEAR]Q2','[YEAR]Q3')
     join date_dim d3
       on cs.cs_sold_date_sk = d3.d_date_sk and d3.d_quarter_name in ('[YEAR]Q1','[YEAR]Q2','[YEAR]Q3')
     join store s
       on s.s_store_sk = ss.ss_store_sk
     join item i
       on i.i_item_sk = ss.ss_item_sk
 group by i_item_id
         ,i_item_desc
         ,s_state
 order by i_item_id
         ,i_item_desc
         ,s_state
[_LIMITC];

