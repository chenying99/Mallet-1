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
[COMMENT] QUERY_ID=40;

 define YEAR=random(1998,2002,uniform);
 define SALES_DATE=date([YEAR]+"-01-31",[YEAR]+"-7-01",sales);
 define _LIMIT=100;

 select  
   w_state
  ,i_item_id
  ,sum(case when (d_date < '[SALES_DATE]') 
        then cs_sales_price - coalesce(cr_refunded_cash,0) else 0.0 end) as sales_before
  ,sum(case when (d_date >= '[SALES_DATE]') 
        then cs_sales_price - coalesce(cr_refunded_cash,0) else 0.0 end) as sales_after
 from
   catalog_sales cs left outer join catalog_returns cr on
       (cs.cs_order_number = cr.cr_order_number 
        and cs.cs_item_sk = cr.cr_item_sk)
   join warehouse w
     on cs.cs_warehouse_sk = w.w_warehouse_sk 
   join item i
     on i.i_current_price between 0.99 and 1.49
     and i.i_item_sk = cs.cs_item_sk
   join date_dim d
     on cs.cs_sold_date_sk = d.d_date_sk
     and d.d_date between date_sub ('[SALES_DATE]',30)
                    and date_add('[SALES_DATE]',30) 
 group by
    w_state,i_item_id
 order by w_state,i_item_id
[_LIMITC];
