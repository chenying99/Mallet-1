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
[COMMENT] QUERY_ID=4;

define YEAR=random(1998,2001,uniform);
define SELECTCONE= text({"t_s_secyear.customer_id",1},{"t_s_secyear.customer_first_name",1},{"t_s_secyear.customer_last_name",1},{"t_s_secyear.customer_preferred_cust_flag",1},{"t_s_secyear.customer_birth_country",1},{"t_s_secyear.customer_login",1},{"t_s_secyear.customer_email_address",1},{"t_s_secyear.customer_id,t_s_secyear.customer_first_name,t_s_secyear.customer_last_name,t_s_secyear.customer_preferred_cust_flag,t_s_secyear.customer_birth_country,t_s_secyear.customer_login,t_s_secyear.customer_email_address",1});
define _LIMIT=100;

-- drop temporary table
drop table if exists year_total_[_STREAM];

-- create temporary table
create table year_total_[_STREAM] (customer_id string, customer_first_name string, customer_last_name string, customer_preferred_cust_flag string,
                         customer_birth_country string, customer_login string, customer_email_address string, dyear bigint,
                         year_total double, sale_type string);
-- the query
insert overwrite table year_total_[_STREAM] select * from (
 select c_customer_id customer_id
       ,c_first_name customer_first_name
       ,c_last_name customer_last_name
       ,c_preferred_cust_flag customer_preferred_cust_flag
       ,c_birth_country customer_birth_country
       ,c_login customer_login
       ,c_email_address customer_email_address
       ,d_year dyear
       ,sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2) year_total
       ,'s' sale_type
 from
      customer c join store_sales ss
        on c.c_customer_sk = ss.ss_customer_sk
      join date_dim d
        on ss.ss_sold_date_sk = d.d_date_sk
 group by c_customer_id
         ,c_first_name
         ,c_last_name
         ,c_preferred_cust_flag
         ,c_birth_country
         ,c_login
         ,c_email_address
         ,d_year
 union all
 select c_customer_id customer_id
       ,c_first_name customer_first_name
       ,c_last_name customer_last_name
       ,c_preferred_cust_flag customer_preferred_cust_flag
       ,c_birth_country customer_birth_country
       ,c_login customer_login
       ,c_email_address customer_email_address
       ,d_year dyear
       ,sum((((cs_ext_list_price-cs_ext_wholesale_cost-cs_ext_discount_amt)+cs_ext_sales_price)/2) ) year_total
       ,'c' sale_type
 from 
      customer c join catalog_sales cs
        on c.c_customer_sk = cs.cs_bill_customer_sk
      join date_dim d
        on cs.cs_sold_date_sk = d.d_date_sk
 group by c_customer_id
         ,c_first_name
         ,c_last_name
         ,c_preferred_cust_flag
         ,c_birth_country
         ,c_login
         ,c_email_address
         ,d_year
union all
 select c_customer_id customer_id
       ,c_first_name customer_first_name
       ,c_last_name customer_last_name
       ,c_preferred_cust_flag customer_preferred_cust_flag
       ,c_birth_country customer_birth_country
       ,c_login customer_login
       ,c_email_address customer_email_address
       ,d_year dyear
       ,sum((((ws_ext_list_price-ws_ext_wholesale_cost-ws_ext_discount_amt)+ws_ext_sales_price)/2) ) year_total
       ,'w' sale_type
 from 
      customer c join web_sales ws
        on c.c_customer_sk = ws.ws_bill_customer_sk
      join date_dim d
        on ws.ws_sold_date_sk = d.d_date_sk
 group by c_customer_id
         ,c_first_name
         ,c_last_name
         ,c_preferred_cust_flag
         ,c_birth_country
         ,c_login
         ,c_email_address
         ,d_year
         ) union_result;

  select  [SELECTCONE]
 from
      year_total_[_STREAM] t_s_firstyear join year_total_[_STREAM] t_s_secyear
        on t_s_secyear.customer_id = t_s_firstyear.customer_id
        and t_s_firstyear.sale_type = 's'
        and t_s_secyear.sale_type = 's'
        and t_s_firstyear.dyear =  [YEAR]
        and t_s_secyear.dyear = [YEAR]+1
        and t_s_firstyear.year_total > 0
      join year_total_[_STREAM] t_c_firstyear
        on t_s_firstyear.customer_id = t_c_firstyear.customer_id
        and t_c_firstyear.sale_type = 'c'
        and t_c_firstyear.dyear =  [YEAR]
        and t_c_firstyear.year_total > 0
      join year_total_[_STREAM] t_c_secyear
        on t_s_firstyear.customer_id = t_c_secyear.customer_id
        and t_c_secyear.sale_type = 'c'
        and t_c_secyear.dyear =  [YEAR]+1
      join year_total_[_STREAM] t_w_firstyear
        on t_s_firstyear.customer_id = t_w_firstyear.customer_id
        and t_w_firstyear.sale_type = 'w'
        and t_w_firstyear.dyear = [YEAR]
        and t_w_firstyear.year_total > 0
      join year_total_[_STREAM] t_w_secyear
        on t_s_firstyear.customer_id = t_w_secyear.customer_id
        and t_w_secyear.sale_type = 'w'
        and t_w_secyear.dyear = [YEAR]+1
 where
       case when t_c_firstyear.year_total > 0 then t_c_secyear.year_total / t_c_firstyear.year_total else null end
           > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else null end
   and case when t_c_firstyear.year_total > 0 then t_c_secyear.year_total / t_c_firstyear.year_total else null end
           > case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else null end
 order by [SELECTCONE]
[_LIMITC];

-- drop temporary table
drop table year_total_[_STREAM];

