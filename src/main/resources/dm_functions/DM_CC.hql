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
  drop table if exists call_center_tmp;
  create table call_center_tmp like call_center;
  
  insert overwrite table call_center_tmp
  select 
    cc_call_center_sk,
    cc_call_center_id,
    cc_rec_start_date,
    case call_center_id is not null when true then date_sub(to_date(from_unixtime(unix_timestamp())), 1)
                                              else cc_rec_end_date end as cc_rec_end_date,
    cc_closed_date_sk,
    cc_open_date_sk,
    cc_name,
    cc_class,
    cc_employees,
    cc_sq_ft,
    cc_hours,
    cc_manager,
    cc_mkt_id,
    cc_mkt_class,
    cc_mkt_desc,
    cc_market_manager,
    cc_division,
    cc_division_name,
    cc_company,
    cc_company_name,
    cc_street_number,
    cc_street_name,
    cc_street_type,
    cc_suite_number,
    cc_city,            
    cc_county,             
    cc_state,               
    cc_zip,                 
    cc_country,             
    cc_gmt_offset,      
    cc_tax_percentage      
  from call_center cc left outer join s_call_center scc
    on scc.call_center_id=cc.cc_call_center_id and cc.cc_rec_end_date is null;

-- insert new rows into the target table
  insert into table call_center_tmp
  select 
    ${ROW_KEY_BASE} + row_number() over (order by call_center_id),
    call_center_id as cc_call_center_id,
    to_date(from_unixtime(unix_timestamp())) as cc_rec_start_date,
    cast(null as string) as cc_rec_end_date,
    d2.d_date_sk as cc_closed_date_sk,
    d1.d_date_sk as cc_open_date_sk,
    call_center_name as cc_name,
    call_center_class as cc_class,
    call_center_employees as cc_employees,
    call_center_sq_ft as cc_sq_ft,
    call_center_hours as cc_hours,
    call_center_manager as cc_manager,
    cc_mkt_id,
    cc_mkt_class,
    cc_mkt_desc,
    cc_market_manager,
    cc_division,
    cc_division_name,
    cc_company,
    cc_company_name,
    cc_street_number,
    cc_street_name,
    cc_street_type,
    cc_suite_number,
    cc_city,            
    cc_county,             
    cc_state,               
    cc_zip,                 
    cc_country,             
    cc_gmt_offset,      
    call_center_tax_percentage as cc_tax_percentage
  from
    s_call_center scc left outer join date_dim d2
      on scc.call_closed_date=d2.d_date
    left outer join date_dim d1
      on scc.call_open_date=d1.d_date
    left outer join call_center cc
      on scc.call_center_id=cc.cc_call_center_id and cc.cc_rec_end_date is null;

drop table call_center; 
alter table call_center_tmp rename to call_center;

