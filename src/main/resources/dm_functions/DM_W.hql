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
drop table if exists warehouse_tmp;
create table warehouse_tmp like warehouse;

insert overwrite table warehouse_tmp
select 
  w_warehouse_sk,
  case wrhs_warehouse_id is not null when true then wrhs_warehouse_id else w_warehouse_id end as w_warehouse_id,
  case wrhs_warehouse_id is not null when true then wrhs_warehouse_desc else w_warehouse_name end as w_warehouse_name,
  case wrhs_warehouse_id is not null when true then wrhs_warehouse_sq_ft else w_warehouse_sq_ft end as w_warehouse_sq_ft,
  w_street_number,
  w_street_name,
  w_street_type, 
  w_suite_number,
  w_city,     
  w_county,       
  w_state,      
  w_zip,       
  w_country,     
  w_gmt_offset
from
  warehouse w left outer join s_warehouse sw
    on w.w_warehouse_id=sw.wrhs_warehouse_id;

drop table warehouse; 
alter table warehouse_tmp rename to warehouse;

