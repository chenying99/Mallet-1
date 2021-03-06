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
[COMMENT] QUERY_ID=30;

 define STATE= dist(fips_county, 3, 1);
 define YEAR= random(1999, 2002, uniform);
 define _LIMIT=100;
 
 -- drop temporary table
drop table if exists customer_total_return_[_STREAM];

 -- create temporary table
create table customer_total_return_[_STREAM] (ctr_customer_sk bigint, ctr_state string, ctr_total_return double);

-- the query
insert overwrite table customer_total_return_[_STREAM]
 select wr_returning_customer_sk as ctr_customer_sk
        ,ca_state as ctr_state, 
    sum(wr_return_amt) as ctr_total_return
 from 
     web_returns wr join date_dim d
       on wr.wr_returned_date_sk = d.d_date_sk 
       and d.d_year =[YEAR]
     join customer_address ca
       on wr.wr_returning_addr_sk = ca.ca_address_sk 
 group by wr_returning_customer_sk
         ,ca_state;


  select  c_customer_id,c_salutation,c_first_name,c_last_name,c_preferred_cust_flag
       ,c_birth_day,c_birth_month,c_birth_year,c_birth_country,c_login,c_email_address
       ,c_last_review_date,ctr_total_return
  from (
         select  c_customer_id,c_salutation,c_first_name,c_last_name,c_preferred_cust_flag
               ,c_birth_day,c_birth_month,c_birth_year,c_birth_country,c_login,c_email_address
               ,c_last_review_date,ctr_total_return, ctr_state
         from 
             customer_total_return_[_STREAM] ctr1 join customer_address ca
               on ca.ca_state = '[STATE]'
             join customer c
               on ca.ca_address_sk = c.c_current_addr_sk
               and ctr1.ctr_customer_sk = c.c_customer_sk)
    tmp1 join (
        select ctr_state, avg(ctr_total_return)*1.2 as avg_return
        from customer_total_return_[_STREAM]
        group by ctr_state) tmp2
        on tmp1.ctr_state = tmp2.ctr_state
    where ctr_total_return > avg_return
    order by c_customer_id,c_salutation,c_first_name,c_last_name,c_preferred_cust_flag
                      ,c_birth_day,c_birth_month,c_birth_year,c_birth_country,c_login,c_email_address
                      ,c_last_review_date,ctr_total_return
    [_LIMITC];

-- drop temporary table
drop table customer_total_return_[_STREAM];

