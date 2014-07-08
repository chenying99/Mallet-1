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
  drop table if exists web_page_tmp;
  create table web_page_tmp like web_page;
  
  insert overwrite table web_page_tmp
  select
    wp_web_page_sk,     
    wp_web_page_id,     
    wp_rec_start_date,  
    case wpag_web_page_id is not null when true then date_sub(to_date(from_unixtime(unix_timestamp())), 1)
                                              else wp_rec_end_date end as wp_rec_end_date,
    wp_creation_date_sk,
    wp_access_date_sk,  
    wp_autogen_flag,    
    wp_customer_sk,     
    wp_url,             
    wp_type,            
    wp_char_count,      
    wp_link_count,      
    wp_image_count,     
    wp_max_ad_count
  from
    web_page wp left outer join s_web_page swp
      on wp.wp_web_page_id=swp.wpag_web_page_id and wp.wp_rec_end_date is null;

-- insert new rows into the target table
  insert into table web_page_tmp
  select 
    ${ROW_KEY_BASE} + row_number() over (order by wpag_web_page_id),
    wpag_web_page_id,      
    to_date(from_unixtime(unix_timestamp())),
    cast(null as string),
    d1.d_date_sk,
    d2.d_date_sk,  
    wpag_autogen_flag,    
    wp_customer_sk,     
    wpag_url,             
    wpag_type,            
    wpag_char_cnt,      
    wpag_link_cnt,      
    wpag_image_cnt,     
    wpag_max_ad_cnt
  from
    s_web_page swp left outer join date_dim d1
      on d1.d_date=swp.wpag_create_date
    left outer join date_dim d2
      on d2.d_date=swp.wpag_access_date
    left outer join web_page wp
      on wp.wp_web_page_id=swp.wpag_web_page_id and wp.wp_rec_end_date is null;    

drop table web_page; 
alter table web_page_tmp rename to web_page;

