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
[COMMENT] QUERY_ID=83;

 define YEAR = random(1998, 2002, uniform);
 define RETURNED_DATE_ONE=date([YEAR]+"-01-01",[YEAR]+"-07-24",sales);
 define RETURNED_DATE_TWO=date([YEAR]+"-08-01",[YEAR]+"-10-24",sales);
 define RETURNED_DATE_THREE=date([YEAR]+"-11-01",[YEAR]+"-11-24",sales);
 define _LIMIT=100;
 
  -- drop temporary table
drop table if exists sr_items_[_STREAM];
drop table if exists cr_items_[_STREAM];
drop table if exists wr_items_[_STREAM];

 -- create temporary table
create table sr_items_[_STREAM] (item_id string, sr_item_qty bigint);
create table cr_items_[_STREAM] (item_id string, cr_item_qty bigint);
create table wr_items_[_STREAM] (item_id string, wr_item_qty bigint);

-- the query
insert overwrite table sr_items_[_STREAM]
 select i_item_id item_id,
        sum(sr_return_quantity) sr_item_qty
 from (
      select i_item_id, sr_return_quantity, d_date
      from
          store_returns sr join item i
            on sr.sr_item_sk = i.i_item_sk
          join date_dim d
            on sr.sr_returned_date_sk   = d.d_date_sk) tmp1
      left semi join (
            select d_date
            from date_dim
                 left semi join
                (select d_week_seq
                from date_dim
                where d_date in ('[RETURNED_DATE_ONE]','[RETURNED_DATE_TWO]',
                '[RETURNED_DATE_THREE]')) tmp3
              on date_dim.d_week_seq = tmp3.d_week_seq
              ) tmp2
            on tmp1.d_date = tmp2.d_date
 group by i_item_id;

insert overwrite table cr_items_[_STREAM]
 select i_item_id item_id,
        sum(cr_return_quantity) cr_item_qty
 from (
      select i_item_id, cr_return_quantity, d_date
      from
          catalog_returns cr join item i
            on cr.cr_item_sk = i.i_item_sk
          join date_dim d
            on cr.cr_returned_date_sk   = d.d_date_sk) tmp1
      left semi join (
            select d_date
            from date_dim
                 left semi join
                (select d_week_seq
                from date_dim
              where d_date in ('[RETURNED_DATE_ONE]','[RETURNED_DATE_TWO]',
              '[RETURNED_DATE_THREE]')) tmp3
              on date_dim.d_week_seq = tmp3.d_week_seq
              ) tmp2
            on tmp1.d_date = tmp2.d_date
 group by i_item_id;

insert overwrite table wr_items_[_STREAM]
 select i_item_id item_id,
        sum(wr_return_quantity) wr_item_qty
 from (
      select i_item_id, wr_return_quantity, d_date
     from 
          web_returns wr join item i
            on wr.wr_item_sk = i.i_item_sk
          join date_dim d
            on wr.wr_returned_date_sk   = d.d_date_sk) tmp1
      left semi join (
            select d_date
            from date_dim
                 left semi join
                (select d_week_seq
                from date_dim
                where d_date in ('[RETURNED_DATE_ONE]','[RETURNED_DATE_TWO]',
                '[RETURNED_DATE_THREE]')) tmp3
              on date_dim.d_week_seq = tmp3.d_week_seq
              ) tmp2
            on tmp1.d_date = tmp2.d_date
 group by i_item_id;

  select  sr_items_[_STREAM].item_id
       ,sr_item_qty
       ,sr_item_qty/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 * 100 sr_dev
       ,cr_item_qty
       ,cr_item_qty/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 * 100 cr_dev
       ,wr_item_qty
       ,wr_item_qty/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 * 100 wr_dev
       ,(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 average
 from 
     sr_items_[_STREAM] join cr_items_[_STREAM]
       on sr_items_[_STREAM].item_id=cr_items_[_STREAM].item_id
     join wr_items_[_STREAM]
       on sr_items_[_STREAM].item_id=wr_items_[_STREAM].item_id 
 order by sr_items_[_STREAM].item_id
         ,sr_item_qty
 [_LIMITC];

 -- drop temporary table
drop table sr_items_[_STREAM];
drop table cr_items_[_STREAM];
drop table wr_items_[_STREAM];

