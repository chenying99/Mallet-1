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
  drop table if exists web_site_tmp;
  create table web_site_tmp like web_site;
  
  insert overwrite table web_site_tmp
  select
    web_site_sk,       
    web_site_id,       
    web_rec_start_date,
    case wsit_web_site_id is not null when true then date_sub(to_date(from_unixtime(unix_timestamp())), 1)
                                              else web_rec_end_date end as web_rec_end_date,
    web_name,          
    web_open_date_sk,  
    web_close_date_sk, 
    web_class,         
    web_manager,       
    web_mkt_id,        
    web_mkt_class,     
    web_mkt_desc,      
    web_market_manager,
    web_company_id,    
    web_company_name,  
    web_street_number, 
    web_street_name,   
    web_street_type,   
    web_suite_number,  
    web_city,          
    web_county,        
    web_state,         
    web_zip,           
    web_country,       
    web_gmt_offset,    
    web_tax_percentage
  from web_site w left outer join s_web_site sw
    on w.web_site_id = sw.wsit_web_site_id and w.web_rec_end_date is null;

-- insert new rows into the target table
  insert into table web_site_tmp
  select 
    ${ROW_KEY_BASE} + row_number() over (order by wsit_web_site_id),
    wsit_web_site_id,      
    to_date(from_unixtime(unix_timestamp())),
    cast(null as string),
    wsit_site_name,          
    d1.d_date_sk,  
    d2.d_date_sk, 
    wsit_site_class,         
    wsit_site_manager,       
    web_mkt_id,        
    web_mkt_class,     
    web_mkt_desc,      
    web_market_manager,
    web_company_id,    
    web_company_name,  
    web_street_number, 
    web_street_name,   
    web_street_type,   
    web_suite_number,  
    web_city,          
    web_county,        
    web_state,         
    web_zip,           
    web_country,       
    web_gmt_offset,    
    wsit_tax_percentage
  from
    s_web_site sw left outer join date_dim d1
      on d1.d_date = sw.wsit_open_date
    left outer join date_dim d2
      on d2.d_date = sw.wsit_closed_date
    left outer join web_site w
    on w.web_site_id = sw.wsit_web_site_id and w.web_rec_end_date is null;

drop table web_site; 
alter table web_site_tmp rename to web_site;

