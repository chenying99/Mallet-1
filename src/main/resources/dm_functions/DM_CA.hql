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

USE mallet_db;
drop table if exists customer_address_tmp;
create table customer_address_tmp like customer_address;

insert overwrite table customer_address_tmp
select 
  ca_address_sk,
  ca_address_id,
  case c_current_addr_sk is not null when true then cast(cust_street_number as string)
                                               else ca_street_number end as ca_street_number,
  case c_current_addr_sk is not null when true then concat(rtrim(cust_street_name1), ' ', rtrim(cust_street_name2))
                                               else ca_street_name end as ca_street_name,
  case c_current_addr_sk is not null when true then cust_street_type else ca_street_type end as ca_street_type,
  case c_current_addr_sk is not null when true then cust_suite_number else ca_suite_number end as ca_suite_number,
  case c_current_addr_sk is not null when true then cust_city else ca_city end as ca_city,
  case c_current_addr_sk is not null when true then cust_county else ca_county end as ca_county,
  case c_current_addr_sk is not null when true then cust_state else ca_state end as ca_state,
  case c_current_addr_sk is not null when true then cust_zip else ca_zip end as ca_zip,
  case c_current_addr_sk is not null when true then cust_country else ca_country end as ca_country,
  case c_current_addr_sk is not null when true then cast(zipg_gmt_offset as double)
                                               else ca_gmt_offset end as ca_gmt_offset,
  case c_current_addr_sk is not null when true then cust_loc_type else ca_location_type end as ca_location_type
from
  customer_address ca left outer join (
    select
      cust_customer_id,
      cust_salutation,
      cust_last_name,
      cust_first_name,
      cust_preffered_flag,
      cust_birth_date,
      cust_birth_country,
      cust_login_id,
      cust_email_address,
      cust_last_login_chg_date,
      cust_first_shipto_date,
      cust_first_purchase_date,
      cust_last_review_date,
      cust_primary_machine_id,
      cust_secondary_machine_id,
      cust_street_number,
      cust_suite_number,
      cust_street_name1,
      cust_street_name2,
      cust_street_type,
      cust_city,
      cust_zip,
      cust_county,
      cust_state,
      cust_country,
      cust_loc_type,
      cust_gender,
      cust_marital_status,
      cust_educ_status,
      cust_credit_rating,
      cust_purch_est,
      cust_buy_potential,
      cust_depend_cnt,
      cust_depend_emp_cnt,
      cust_depend_college_cnt,
      cust_vehicle_cnt,
      cust_annual_income,
      zipg_gmt_offset,
      c_current_addr_sk
    from (
      select
        cust_customer_id,
        cust_salutation,
        cust_last_name,
        cust_first_name,
        cust_preffered_flag,
        cust_birth_date,
        cust_birth_country,
        cust_login_id,
        cust_email_address,
        cust_last_login_chg_date,
        cust_first_shipto_date,
        cust_first_purchase_date,
        cust_last_review_date,
        cust_primary_machine_id,
        cust_secondary_machine_id,
        cust_street_number,
        cust_suite_number,
        cust_street_name1,
        cust_street_name2,
        cust_street_type,
        cust_city,
        cust_zip,
        cust_county,
        cust_state,
        cust_country,
        cust_loc_type,
        cust_gender,
        cust_marital_status,
        cust_educ_status,
        cust_credit_rating,
        cust_purch_est,
        cust_buy_potential,
        cust_depend_cnt,
        cust_depend_emp_cnt,
        cust_depend_college_cnt,
        cust_vehicle_cnt,
        cust_annual_income,
        zipg_gmt_offset,
        c_current_addr_sk,
--different customers may have same address
        row_number() over (partition by c_current_addr_sk) as seq
      from
        s_customer sc join s_zip_to_gmt s
          on sc.cust_zip=s.zipg_zip
        join customer c
          on sc.cust_customer_id=c.c_customer_id) y
--arbitrarily choose one from the customers having same address
    where seq = 1) x
  on x.c_current_addr_sk=ca.ca_address_sk;

drop table customer_address; 
alter table customer_address_tmp rename to customer_address;

