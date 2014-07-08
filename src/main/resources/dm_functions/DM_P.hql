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
drop table if exists promotion_tmp;
create table promotion_tmp like promotion;

insert overwrite table promotion_tmp
select 
  p_promo_sk,
  p_promo_id,
  case prom_promotion_id is not null when true then start_date_sk else p_start_date_sk end as p_start_date_sk,
  case prom_promotion_id is not null when true then end_date_sk else p_end_date_sk end as p_end_date_sk,
  p_item_sk,
  case prom_promotion_id is not null when true then prom_cost else p_cost end as p_cost,
  case prom_promotion_id is not null when true then cast(prom_response_target as bigint)
                                               else p_response_target end as p_response_target,
  case prom_promotion_id is not null when true then prom_promotion_name else p_promo_name end as p_promo_name,
  case prom_promotion_id is not null when true then prom_channel_dmail else p_channel_dmail end as p_channel_dmail,
  case prom_promotion_id is not null when true then prom_channel_email else p_channel_email end as p_channel_email,
  case prom_promotion_id is not null when true then prom_channel_catalog else p_channel_catalog end as p_channel_catalog,
  case prom_promotion_id is not null when true then prom_channel_tv else p_channel_tv end as p_channel_tv,
  case prom_promotion_id is not null when true then prom_channel_radio else p_channel_radio end as p_channel_radio,
  case prom_promotion_id is not null when true then prom_channel_press else p_channel_press end as p_channel_press,
  case prom_promotion_id is not null when true then prom_channel_event else p_channel_event end as p_channel_event,
  case prom_promotion_id is not null when true then prom_channel_demo else p_channel_demo end as p_channel_demo,
  case prom_promotion_id is not null when true then prom_channel_details else p_channel_details end as p_channel_details,
  case prom_promotion_id is not null when true then prom_purpose else p_purpose end as p_purpose,
  case prom_promotion_id is not null when true then prom_discount_active else p_discount_active end as p_discount_active
from
  promotion p left outer join (
    select
      prom_promotion_id,
      prom_promotion_name,
      prom_start_date,
      prom_end_date,
      prom_cost,
      prom_response_target,
      prom_channel_dmail,
      prom_channel_email,
      prom_channel_catalog,
      prom_channel_tv,
      prom_channel_radio,
      prom_channel_press,
      prom_channel_event,
      prom_channel_demo,
      prom_channel_details,
      prom_purpose,
      prom_discount_active,
      prom_discount_pct,
      d1.d_date_sk start_date_sk,
      d2.d_date_sk end_date_sk
    from
      s_promotion sp left outer join date_dim d1
        on sp.prom_start_date=d1.d_date
      left outer join date_dim d2
        on sp.prom_end_date=d2.d_date) x
    on x.prom_promotion_id=p.p_promo_id;

drop table promotion; 
alter table promotion_tmp rename to promotion;
