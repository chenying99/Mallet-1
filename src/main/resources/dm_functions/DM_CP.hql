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
drop table if exists catalog_page_tmp;
create table catalog_page_tmp like catalog_page;
  
insert overwrite table catalog_page_tmp
select 
  cp_catalog_page_sk,
  case cpag_id is not null when true then cpag_id else cp_catalog_page_id end as cp_catalog_page_id,
  case cpag_id is not null when true then start_date_sk else cp_start_date_sk end as cp_start_date_sk,
  case cpag_id is not null when true then end_date_sk else cp_end_date_sk end as cp_end_date_sk,
  case cpag_id is not null when true then cpag_department else cp_department end as cp_department,
  case cpag_id is not null when true then cpag_catalog_number else cp_catalog_number end as cp_catalog_number,
  case cpag_id is not null when true then cpag_catalog_page_number else cp_catalog_page_number end as cp_catalog_page_number,
  case cpag_id is not null when true then cpag_description else cp_description end as cp_description,
  case cpag_id is not null when true then cpag_type else cp_type end as cp_type
from
  catalog_page cp left outer join (
    select
      cpag_catalog_number,
      cpag_catalog_page_number,
      cpag_department,
      cpag_id,
      cpag_start_date,
      cpag_end_date,
      cpag_description,
      cpag_type,
      startd.d_date_sk as start_date_sk,
      endd.d_date_sk as end_date_sk
    from
      s_catalog_page scp join date_dim startd
        on scp.cpag_start_date=startd.d_date
      join date_dim endd
        on scp.cpag_end_date=endd.d_date) x
    on x.cpag_id=cp.cp_catalog_page_id;

drop table catalog_page; 
alter table catalog_page_tmp rename to catalog_page;

