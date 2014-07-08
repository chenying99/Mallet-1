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
[COMMENT] QUERY_ID=75;

define CINDX = random(1,rowcount("categories"),uniform);
define CATEGORY = distmember(categories,[CINDX],1);
define YEAR = random(1999,2002,uniform);
define _LIMIT=100;

 -- drop temporary table
drop table if exists all_sales_[_STREAM];

-- create temporary table
create table all_sales_[_STREAM] (d_year bigint, i_brand_id bigint, i_class_id bigint, i_category_id bigint,
                        i_manufact_id bigint, sales_cnt bigint, sales_amt double);

-- the query
insert overwrite table all_sales_[_STREAM]
 SELECT d_year
       ,i_brand_id
       ,i_class_id
       ,i_category_id
       ,i_manufact_id
       ,SUM(sales_cnt) AS sales_cnt
       ,SUM(sales_amt) AS sales_amt
 FROM (
       SELECT DISTINCT d_year
                 ,i_brand_id
                 ,i_class_id
                 ,i_category_id
                 ,i_manufact_id
                 ,sales_cnt
                 ,sales_amt
       FROM (
           SELECT d_year
                 ,i_brand_id
                 ,i_class_id
                 ,i_category_id
                 ,i_manufact_id
                 ,cs_quantity - COALESCE(cr_return_quantity,0) AS sales_cnt
                 ,cs_ext_sales_price - COALESCE(cr_return_amount,0.0) AS sales_amt
           FROM catalog_sales cs JOIN item i ON i.i_item_sk=cs.cs_item_sk
                              JOIN date_dim d ON d.d_date_sk=cs.cs_sold_date_sk
                              LEFT OUTER JOIN catalog_returns cr ON (cs.cs_order_number=cr.cr_order_number 
                                                        AND cs.cs_item_sk=cr.cr_item_sk)
           WHERE i_category='[CATEGORY]'
           UNION ALL
           SELECT d_year
                 ,i_brand_id
                 ,i_class_id
                 ,i_category_id
                 ,i_manufact_id
                 ,ss_quantity - COALESCE(sr_return_quantity,0) AS sales_cnt
                 ,ss_ext_sales_price - COALESCE(sr_return_amt,0.0) AS sales_amt
           FROM store_sales ss JOIN item i ON i.i_item_sk=ss.ss_item_sk
                            JOIN date_dim d ON d.d_date_sk=ss.ss_sold_date_sk
                            LEFT OUTER JOIN store_returns sr ON (ss.ss_ticket_number=sr.sr_ticket_number 
                                                    AND ss.ss_item_sk=sr.sr_item_sk)
           WHERE i_category='[CATEGORY]'
           UNION ALL
           SELECT d_year
                 ,i_brand_id
                 ,i_class_id
                 ,i_category_id
                 ,i_manufact_id
                 ,ws_quantity - COALESCE(wr_return_quantity,0) AS sales_cnt
                 ,ws_ext_sales_price - COALESCE(wr_return_amt,0.0) AS sales_amt
           FROM web_sales ws JOIN item i ON i.i_item_sk=ws.ws_item_sk
                          JOIN date_dim d ON d.d_date_sk=ws.ws_sold_date_sk
                          LEFT OUTER JOIN web_returns wr ON (ws.ws_order_number=wr.wr_order_number 
                                                AND ws.ws_item_sk=wr.wr_item_sk)
           WHERE i_category='[CATEGORY]') tmp ) sales_detail
 GROUP BY d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id;

 SELECT  prev_yr.d_year AS prev_year
                          ,curr_yr.d_year AS year
                          ,curr_yr.i_brand_id
                          ,curr_yr.i_class_id
                          ,curr_yr.i_category_id
                          ,curr_yr.i_manufact_id
                          ,prev_yr.sales_cnt AS prev_yr_cnt
                          ,curr_yr.sales_cnt AS curr_yr_cnt
                          ,curr_yr.sales_cnt-prev_yr.sales_cnt AS sales_cnt_diff
                          ,curr_yr.sales_amt-prev_yr.sales_amt AS sales_amt_diff
 FROM all_sales_[_STREAM] curr_yr join all_sales_[_STREAM] prev_yr
   ON curr_yr.i_brand_id=prev_yr.i_brand_id
   AND curr_yr.i_class_id=prev_yr.i_class_id
   AND curr_yr.i_category_id=prev_yr.i_category_id
   AND curr_yr.i_manufact_id=prev_yr.i_manufact_id
   AND curr_yr.d_year=[YEAR]
   AND prev_yr.d_year=[YEAR]-1
 WHERE CAST(curr_yr.sales_cnt AS double)/CAST(prev_yr.sales_cnt AS double)<0.9
 ORDER BY sales_cnt_diff
 [_LIMITC];

 -- drop temporary table
drop table all_sales_[_STREAM];

