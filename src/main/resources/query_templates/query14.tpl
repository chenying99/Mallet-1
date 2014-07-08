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
[COMMENT] QUERY_ID=14;

 define YEAR= random(1998, 2000, uniform);
 define DAY = random(1,28,uniform);
 define _LIMIT=100; 

-- drop temporary table
drop table if exists cross_items_[_STREAM];
drop table if exists avg_sales_[_STREAM];

-- create temporary table
create table cross_items_[_STREAM] (ss_item_sk bigint);
create table avg_sales_[_STREAM] (average_sales double);

-- the query
insert overwrite table cross_items_[_STREAM]
 select i_item_sk ss_item_sk
 from item i join
 (select distinct brand_id, class_id, category_id
  from
     (select iss.i_brand_id brand_id
         ,iss.i_class_id class_id
         ,iss.i_category_id category_id
     from 
         store_sales ss join item iss
           on ss.ss_item_sk = iss.i_item_sk
         join date_dim d1
           on ss.ss_sold_date_sk = d1.d_date_sk
           and d1.d_year between 1999 AND 1999 + 2) tmp1
   left semi join ( 
     select ics.i_brand_id
         ,ics.i_class_id
         ,ics.i_category_id
     from 
         catalog_sales cs join item ics
           on cs.cs_item_sk = ics.i_item_sk
         join date_dim d2
           on cs.cs_sold_date_sk = d2.d_date_sk
           and d2.d_year between 1999 AND 1999 + 2) tmp2
     on tmp1.brand_id = tmp2.i_brand_id
     and tmp1.class_id = tmp2.i_class_id
     and tmp1.category_id = tmp2.i_category_id
   left semi join ( 
     select iws.i_brand_id
         ,iws.i_class_id
         ,iws.i_category_id
     from 
         web_sales ws join item iws
           on ws.ws_item_sk = iws.i_item_sk
         join date_dim d3
           on ws.ws_sold_date_sk = d3.d_date_sk
           and d3.d_year between 1999 AND 1999 + 2) tmp3
     on tmp1.brand_id = tmp3.i_brand_id
     and tmp1.class_id = tmp3.i_class_id
     and tmp1.category_id = tmp3.i_category_id
   ) x
   on i.i_brand_id = x.brand_id
   and i.i_class_id = x.class_id
   and i.i_category_id = x.category_id;

insert overwrite table avg_sales_[_STREAM]
  select avg(quantity*list_price) average_sales
  from (select ss_quantity quantity
             ,ss_list_price list_price
       from store_sales ss join date_dim d
         on ss.ss_sold_date_sk = d.d_date_sk
         and d.d_year between 1999 and 2001 
       union all 
       select cs_quantity quantity 
             ,cs_list_price list_price
       from catalog_sales cs join date_dim d
         on cs.cs_sold_date_sk = d.d_date_sk
         and d.d_year between [YEAR] and [YEAR] + 2 
       union all
       select ws_quantity quantity
             ,ws_list_price list_price
       from web_sales ws join date_dim d
         on ws.ws_sold_date_sk = d.d_date_sk
         and d.d_year between [YEAR] and [YEAR] + 2) x;
         

  select  channel, i_brand_id,i_class_id,i_category_id,sum(sales), sum(number_sales)
 from(
       select channel, i_brand_id, i_class_id, i_category_id, sales, number_sales
       from
           (select 'store' channel, i_brand_id,i_class_id
                 ,i_category_id,sum(ss_quantity*ss_list_price) sales
                 , count(*) number_sales
           from 
               store_sales ss join item i
                 on ss.ss_item_sk = i.i_item_sk
               join date_dim d
                 on ss.ss_sold_date_sk = d.d_date_sk
                 and d.d_year = [YEAR]+2 
                 and d.d_moy = 11
               left semi join (select ss_item_sk from cross_items_[_STREAM]) tmp
                 on ss.ss_item_sk = tmp.ss_item_sk
           group by i_brand_id,i_class_id,i_category_id) tmp1
         join
           (select average_sales from avg_sales_[_STREAM]) tmp2 
       where sales > average_sales
       union all
       select channel, i_brand_id, i_class_id, i_category_id, sales, number_sales
       from
           (select 'catalog' channel, i_brand_id,i_class_id,i_category_id, sum(cs_quantity*cs_list_price) sales, count(*) number_sales
           from 
               catalog_sales cs join item i
                 on cs.cs_item_sk = i.i_item_sk
               join date_dim d
                 on cs.cs_sold_date_sk = d.d_date_sk
                 and d.d_year = [YEAR]+2 
                 and d.d_moy = 11
               left semi join  (select ss_item_sk from cross_items_[_STREAM]) tmp
                 on cs.cs_item_sk = tmp.ss_item_sk
           group by i_brand_id,i_class_id,i_category_id) tmp1
         join
           (select average_sales from avg_sales_[_STREAM]) tmp2 
       where sales > average_sales
       union all
       select channel, i_brand_id, i_class_id, i_category_id, sales, number_sales
       from
           (select 'web' channel, i_brand_id,i_class_id,i_category_id, sum(ws_quantity*ws_list_price) sales , count(*) number_sales
           from 
               web_sales ws join item i
                 on ws.ws_item_sk = i.i_item_sk
               join date_dim d
                 on ws.ws_sold_date_sk = d.d_date_sk
                 and d.d_year = [YEAR]+2
                 and d.d_moy = 11
               left semi join  (select ss_item_sk from cross_items_[_STREAM]) tmp
                 on ws.ws_item_sk = tmp.ss_item_sk
           group by i_brand_id,i_class_id,i_category_id) tmp1
         join
           (select average_sales from avg_sales_[_STREAM]) tmp2 
       where sales > average_sales
 ) y
 group by channel, i_brand_id,i_class_id,i_category_id with rollup
 order by channel,i_brand_id,i_class_id,i_category_id
 [_LIMITC];

insert overwrite table cross_items_[_STREAM]
 select i_item_sk ss_item_sk
 from item i join
 (select distinct brand_id, class_id, category_id
  from
     (select iss.i_brand_id brand_id
         ,iss.i_class_id class_id
         ,iss.i_category_id category_id
     from 
         store_sales ss join item iss
           on ss.ss_item_sk = iss.i_item_sk
         join date_dim d1
           on ss.ss_sold_date_sk = d1.d_date_sk
           and d1.d_year between 1999 AND 1999 + 2) tmp1
   left semi join ( 
     select ics.i_brand_id
         ,ics.i_class_id
         ,ics.i_category_id
     from 
         catalog_sales cs join item ics
           on cs.cs_item_sk = ics.i_item_sk
         join date_dim d2
           on cs.cs_sold_date_sk = d2.d_date_sk
           and d2.d_year between 1999 AND 1999 + 2) tmp2
     on tmp1.brand_id = tmp2.i_brand_id
     and tmp1.class_id = tmp2.i_class_id
     and tmp1.category_id = tmp2.i_category_id
   left semi join ( 
     select iws.i_brand_id
         ,iws.i_class_id
         ,iws.i_category_id
     from 
         web_sales ws join item iws
           on ws.ws_item_sk = iws.i_item_sk
         join date_dim d3
           on ws.ws_sold_date_sk = d3.d_date_sk
           and d3.d_year between 1999 AND 1999 + 2) tmp3
     on tmp1.brand_id = tmp3.i_brand_id
     and tmp1.class_id = tmp3.i_class_id
     and tmp1.category_id = tmp3.i_category_id
   ) x
   on i.i_brand_id = x.brand_id
   and i.i_class_id = x.class_id
   and i.i_category_id = x.category_id;

insert overwrite table avg_sales_[_STREAM]
select avg(quantity*list_price) average_sales
  from (select ss_quantity quantity
             ,ss_list_price list_price
       from store_sales ss join date_dim d
         on ss.ss_sold_date_sk = d.d_date_sk
         and d.d_year between [YEAR] and [YEAR] + 2
       union all
       select cs_quantity quantity
             ,cs_list_price list_price
       from catalog_sales cs join date_dim d
         on cs.cs_sold_date_sk = d.d_date_sk
         and d.d_year between [YEAR] and [YEAR] + 2
       union all
       select ws_quantity quantity
             ,ws_list_price list_price
       from web_sales ws join date_dim d
         on ws.ws_sold_date_sk = d.d_date_sk
         and d.d_year between [YEAR] and [YEAR] + 2) x;

  select  * from
 (select channel, i_brand_id, i_class_id, i_category_id, sales, number_sales
  from
     (select 'store' channel, i_brand_id,i_class_id,i_category_id
            ,sum(ss_quantity*ss_list_price) sales, count(*) number_sales
     from 
         store_sales ss join item i
           on ss.ss_item_sk = i.i_item_sk
         join date_dim d
           on ss.ss_sold_date_sk = d.d_date_sk
         left semi join (select ss_item_sk from cross_items_[_STREAM]) tmp1
           on ss.ss_item_sk = tmp1.ss_item_sk
         left semi join (select d_week_seq
                         from date_dim
                         where d_year = [YEAR] + 1
                           and d_moy = 12
                           and d_dom = [DAY]) tmp2
           on d.d_week_seq = tmp2.d_week_seq
     group by i_brand_id,i_class_id,i_category_id) tmp3
   join
     (select average_sales from avg_sales_[_STREAM]) tmp4
  where sales > average_sales) this_year join
 (select channel, i_brand_id, i_class_id, i_category_id, sales, number_sales
  from
     (select 'store' channel, i_brand_id,i_class_id
            ,i_category_id, sum(ss_quantity*ss_list_price) sales, count(*) number_sales
     from 
         store_sales ss join item i
           on ss.ss_item_sk = i.i_item_sk
         join date_dim d
           on ss.ss_sold_date_sk = d.d_date_sk
         left semi join (select ss_item_sk from cross_items_[_STREAM]) tmp1
           on ss.ss_item_sk = tmp1.ss_item_sk
         left semi join (select d_week_seq
                         from date_dim
                         where d_year = [YEAR]
                           and d_moy = 12
                           and d_dom = [DAY]) tmp2
           on d.d_week_seq = tmp2.d_week_seq
     group by i_brand_id,i_class_id,i_category_id) tmp3
   join
     (select average_sales from avg_sales_[_STREAM]) tmp4
  where sales > average_sales) last_year
   on this_year.i_brand_id= last_year.i_brand_id
   and this_year.i_class_id = last_year.i_class_id
   and this_year.i_category_id = last_year.i_category_id
 order by this_year.channel, this_year.i_brand_id, this_year.i_class_id, this_year.i_category_id
 [_LIMITC];

-- drop temporary table
drop table cross_items_[_STREAM];
drop table avg_sales_[_STREAM];

