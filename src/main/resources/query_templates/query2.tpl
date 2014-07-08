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
[COMMENT] QUERY_ID=2;

 define COUNTY=random(1, rowcount("active_counties", "store"), uniform);
 define GMT=distmember(fips_county,[COUNTY], 6);
 define YEAR=random(1998,2001,uniform);

 -- drop temporary table
drop table if exists wscs_[_STREAM];
drop table if exists wswscs_[_STREAM];

-- create temporary table
create table wscs_[_STREAM] (sold_date_sk bigint, sales_price double);
create table wswscs_[_STREAM] (d_week_seq bigint, sun_sales double, mon_sales double, tue_sales double,
                     wed_sales double, thu_sales double, fri_sales double, sat_sales double);

-- the query
insert overwrite table wscs_[_STREAM]
  select sold_date_sk
        ,sales_price
  from  (select ws_sold_date_sk sold_date_sk
              ,ws_ext_sales_price sales_price
        from web_sales
        union all
        select cs_sold_date_sk sold_date_sk
              ,cs_ext_sales_price sales_price
        from catalog_sales) x;

insert overwrite table wswscs_[_STREAM] 
 select d_week_seq,
        sum(case when (d_day_name='Sunday') then sales_price else null end) sun_sales,
        sum(case when (d_day_name='Monday') then sales_price else null end) mon_sales,
        sum(case when (d_day_name='Tuesday') then sales_price else  null end) tue_sales,
        sum(case when (d_day_name='Wednesday') then sales_price else null end) wed_sales,
        sum(case when (d_day_name='Thursday') then sales_price else null end) thu_sales,
        sum(case when (d_day_name='Friday') then sales_price else null end) fri_sales,
        sum(case when (d_day_name='Saturday') then sales_price else null end) sat_sales
 from wscs_[_STREAM] join date_dim
      on date_dim.d_date_sk = wscs_[_STREAM].sold_date_sk
 group by d_week_seq;
 
 select d_week_seq1
       ,round(sun_sales1/sun_sales2,2)
       ,round(mon_sales1/mon_sales2,2)
       ,round(tue_sales1/tue_sales2,2)
       ,round(wed_sales1/wed_sales2,2)
       ,round(thu_sales1/thu_sales2,2)
       ,round(fri_sales1/fri_sales2,2)
       ,round(sat_sales1/sat_sales2,2)
 from
 (select wswscs_[_STREAM].d_week_seq d_week_seq1
        ,sun_sales sun_sales1
        ,mon_sales mon_sales1
        ,tue_sales tue_sales1
        ,wed_sales wed_sales1
        ,thu_sales thu_sales1
        ,fri_sales fri_sales1
        ,sat_sales sat_sales1
  from wswscs_[_STREAM] join date_dim 
    on date_dim.d_week_seq = wswscs_[_STREAM].d_week_seq and
       date_dim.d_year = [YEAR]) y join
 (select wswscs_[_STREAM].d_week_seq d_week_seq2
        ,sun_sales sun_sales2
        ,mon_sales mon_sales2
        ,tue_sales tue_sales2
        ,wed_sales wed_sales2
        ,thu_sales thu_sales2
        ,fri_sales fri_sales2
        ,sat_sales sat_sales2
  from wswscs_[_STREAM] join date_dim 
    on date_dim.d_week_seq = wswscs_[_STREAM].d_week_seq and
       date_dim.d_year = [YEAR]+1) z
  on y.d_week_seq1=z.d_week_seq2-53
 order by d_week_seq1;
 
 -- drop temporary table
drop table wscs_[_STREAM];
drop table wswscs_[_STREAM];

