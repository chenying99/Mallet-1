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
DROP VIEW IF EXISTS srv;
CREATE VIEW srv AS
SELECT 
  d_date_sk sr_return_date_sk
  ,t_time_sk sr_return_time_sk
  ,i_item_sk sr_item_sk
  ,c.c_customer_sk sr_customer_sk
  ,c.c_current_cdemo_sk sr_cdemo_sk
  ,c.c_current_hdemo_sk sr_hdemo_sk
  ,c.c_current_addr_sk sr_addr_sk
  ,s_store_sk sr_store_sk 
  ,r_reason_sk sr_reason_sk
  ,sret_ticket_number sr_ticket_number
  ,sret_return_qty sr_return_quantity
  ,sret_return_amt sr_return_amt
  ,sret_return_tax sr_return_tax
  ,sret_return_amt + sret_return_tax sr_return_amt_inc_tax
  ,sret_return_fee sr_fee
  ,sret_return_ship_cost sr_return_ship_cost
  ,sret_refunded_cash sr_refunded_cash
  ,sret_reversed_charge sr_reversed_charge
  ,sret_store_credit sr_store_credit
  ,sret_return_amt+sret_return_tax+sret_return_fee
   -sret_refunded_cash-sret_reversed_charge-sret_store_credit sr_net_loss
FROM s_store_returns s
LEFT OUTER JOIN date_dim d ON (s.sret_return_date = d.d_date)
LEFT OUTER JOIN time_dim t ON (hour(s.sret_return_time) = t.t_hour AND minute(s.sret_return_time) = t.t_minute AND second(s.sret_return_time) = t.t_second)
LEFT OUTER JOIN item i ON (s.sret_item_id = i.i_item_id)
LEFT OUTER JOIN customer c ON (s.sret_customer_id = c.c_customer_id)
LEFT OUTER JOIN store st ON (s.sret_store_id = st.s_store_id)
LEFT OUTER JOIN reason r ON (s.sret_reason_id = r.r_reason_id)
WHERE i_rec_end_date IS NULL AND s_rec_end_date IS NULL;

INSERT INTO TABLE store_returns SELECT * FROM srv;
DROP VIEW srv;

