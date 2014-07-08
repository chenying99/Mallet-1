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
  drop table if exists item_tmp;
  create table item_tmp like item;
  
  insert overwrite table item_tmp
  select
    i_item_sk,     
    i_item_id,      
    i_rec_start_date,
    case item_item_id is not null when true then date_sub(to_date(from_unixtime(unix_timestamp())), 1)
                                              else i_rec_end_date end as i_rec_end_date,
    i_item_desc,     
    i_current_price, 
    i_wholesale_cost,
    i_brand_id,      
    i_brand,         
    i_class_id,      
    i_class,         
    i_category_id,   
    i_category,      
    i_manufact_id,   
    i_manufact,      
    i_size,          
    i_formulation,   
    i_color,         
    i_units,         
    i_container,     
    i_manager_id,    
    i_product_name
  from item i left outer join s_item si
    on i.i_item_id = si.item_item_id and i.i_rec_end_date is NULL;
    
-- insert new rows into the target table
  insert into table item_tmp
  select 
    ${ROW_KEY_BASE} + row_number() over (order by item_item_id),
    item_item_id,      
    to_date(from_unixtime(unix_timestamp())) as i_rec_start_date,
    cast(null as string) as i_rec_end_date,
    item_item_description,     
    item_list_price, 
    item_wholesale_cost,
    i_brand_id,      
    i_brand,         
    i_class_id,      
    i_class,         
    i_category_id,   
    i_category,      
    i_manufact_id,   
    i_manufact,      
    item_size,          
    item_formulation,   
    item_color,         
    item_units,         
    item_container,     
    item_manager_id,    
    i_product_name
  from s_item si left outer join item i
    on i.i_item_id = si.item_item_id and i.i_rec_end_date is NULL;

drop table item; 
alter table item_tmp rename to item;

