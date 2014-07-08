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
drop table if exists customer_tmp;
create table customer_tmp like customer;

insert overwrite table customer_tmp
select 
  c_customer_sk,
  c_customer_id,
  case cust_customer_id is not null when true then cd_demo_sk else c_current_cdemo_sk end as c_current_cdemo_sk,
  case cust_customer_id is not null when true then hd_demo_sk else c_current_hdemo_sk end as c_current_hdemo_sk,
  case cust_customer_id is not null when true then ca_address_sk else c_current_addr_sk end as c_current_addr_sk,
  case cust_customer_id is not null when true then first_shipto_date_sk else c_first_shipto_date_sk end as c_first_shipto_date_sk,
  case cust_customer_id is not null when true then first_sales_date_sk else c_first_sales_date_sk end as c_first_sales_date_sk,
  case cust_customer_id is not null when true then cust_salutation else c_salutation end as c_salutation,
  case cust_customer_id is not null when true then cust_first_name else c_first_name end as c_first_name,
  case cust_customer_id is not null when true then cust_last_name else c_last_name end as c_last_name,
  case cust_customer_id is not null when true then cust_preffered_flag else c_preferred_cust_flag end as c_preferred_cust_flag,
  case cust_customer_id is not null when true then cast(day(cust_birth_date) as bigint)
                                              else c_birth_day end as c_birth_day,
  case cust_customer_id is not null when true then cast(month(cust_birth_date) as bigint)
                                              else c_birth_month end as c_birth_month,
  case cust_customer_id is not null when true then cast(year(cust_birth_date) as bigint)
                                              else c_birth_year end as c_birth_year,
  case cust_customer_id is not null when true then cust_birth_country else c_birth_country end as c_birth_country,
  case cust_customer_id is not null when true then cust_login_id else c_login end as c_login,
  case cust_customer_id is not null when true then cust_email_address else c_email_address end as c_email_address,
  case cust_customer_id is not null when true then cust_last_review_date else c_last_review_date end as c_last_review_date
from
  customer c left outer join (
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
      cd_demo_sk,
      hd_demo_sk,
      d1.d_date_sk first_shipto_date_sk,
      d2.d_date_sk first_sales_date_sk
    from
      s_customer sc join customer_demographics cd
        on sc.cust_gender=cd.cd_gender and
           sc.cust_marital_status=cd.cd_marital_status and
           sc.cust_educ_status=cd.cd_education_status and
           sc.cust_purch_est=cd.cd_purchase_estimate and
           sc.cust_credit_rating=cd.cd_credit_rating and
           sc.cust_depend_cnt=cd.cd_dep_count and
           sc.cust_depend_emp_cnt=cd.cd_dep_employed_count and
           sc.cust_depend_college_cnt=cd.cd_dep_college_count
      join income_band ib
      join household_demographics hd
        on hd.hd_income_band_sk=ib.ib_income_band_sk and
           sc.cust_buy_potential=hd.hd_buy_potential and
           sc.cust_depend_cnt=hd.hd_dep_count and
           sc.cust_vehicle_cnt=hd.hd_vehicle_count
      join date_dim d1
        on d1.d_date=sc.cust_first_purchase_date
      join date_dim d2
        on d2.d_date=sc.cust_first_shipto_date
    where round(cust_annual_income, 0) between ib_lower_bound and ib_upper_bound
        ) x
    on c.c_customer_id=x.cust_customer_id
  left outer join customer_address ca
    on c.c_current_addr_sk=ca.ca_address_sk;

drop table customer; 
alter table customer_tmp rename to customer;

