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
  drop table if exists store_tmp;
  create table store_tmp like store;
  
  insert overwrite table store_tmp
  select
    s_store_sk,        
    s_store_id,        
    s_rec_start_date,  
    case stor_store_id is not null when true then date_sub(to_date(from_unixtime(unix_timestamp())), 1)
                                              else s_rec_end_date end as s_rec_end_date,
    s_closed_date_sk,  
    s_store_name,      
    s_number_employees,
    s_floor_space,     
    s_hours,           
    s_manager,         
    s_market_id,       
    s_geography_class, 
    s_market_desc,     
    s_market_manager,  
    s_division_id,     
    s_division_name,   
    s_company_id,      
    s_company_name,    
    s_street_number,   
    s_street_name,     
    s_street_type,     
    s_suite_number,    
    s_city,            
    s_county,          
    s_state,           
    s_zip,             
    s_country,         
    s_gmt_offset,      
    s_tax_precentage
  from
    store s left outer join s_store ss
      on s.s_store_id=ss.stor_store_id and s.s_rec_end_date is null;

-- insert new rows into the target table
  insert into table store_tmp
  select 
    ${ROW_KEY_BASE} + row_number() over (order by stor_store_id),
    stor_store_id,      
    to_date(from_unixtime(unix_timestamp())),
    cast(null as string),
    d1.d_date_sk,  
    stor_name,      
    stor_employees,
    stor_floor_space,     
    stor_hours,           
    stor_store_manager,         
    stor_market_id,       
    stor_geography_class, 
    s_market_desc,     
    stor_market_manager,  
    s_division_id,     
    s_division_name,   
    s_company_id,      
    s_company_name,    
    s_street_number,   
    s_street_name,     
    s_street_type,     
    s_suite_number,    
    s_city,            
    s_county,          
    s_state,           
    s_zip,             
    s_country,         
    s_gmt_offset,      
    stor_tax_percentage
  from
    s_store ss left outer join store s
      on s.s_store_id=ss.stor_store_id and s.s_rec_end_date is null
    left outer join date_dim d1
      on ss.stor_closed_date=d1.d_date;

drop table store; 
alter table store_tmp rename to store;

