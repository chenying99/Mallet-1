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
DROP VIEW IF EXISTS crv;
CREATE VIEW crv AS
SELECT d_date_sk cr_return_date_sk
      ,t_time_sk cr_return_time_sk
      ,i_item_sk cr_item_sk
      ,c1.c_customer_sk cr_refunded_customer_sk
      ,c1.c_current_cdemo_sk cr_refunded_cdemo_sk
      ,c1.c_current_hdemo_sk cr_refunded_hdemo_sk
      ,c1.c_current_addr_sk cr_refunded_addr_sk
      ,c2.c_customer_sk cr_returning_customer_sk
      ,c2.c_current_cdemo_sk cr_returning_cdemo_sk
      ,c2.c_current_hdemo_sk cr_returning_hdemo_sk
      ,c2.c_current_addr_sk cr_returing_addr_sk
      ,cc_call_center_sk cr_call_center_sk
      ,cp_catalog_page_sk cr_catalog_page_sk
      ,sm_ship_mode_sk cr_ship_mode_sk
      ,w_warehouse_sk cr_warehouse_sk      
      ,r_reason_sk cr_reason_sk
      ,cret_order_id cr_order_number
      ,cret_return_qty cr_return_quantity
      ,cret_return_amt cr_return_amt
      ,cret_return_tax cr_return_tax
      ,cret_return_amt + cret_return_tax cr_return_amt_inc_tax
      ,cret_return_fee cr_fee
      ,cret_return_ship_cost cr_return_ship_cost
      ,cret_refunded_cash cr_refunded_cash
      ,cret_reversed_charge cr_reversed_charge
      ,cret_merchant_credit cr_store_credit
      ,cret_return_amt+cret_return_tax+cret_return_fee
       -cret_refunded_cash-cret_reversed_charge-cret_merchant_credit cr_net_loss
FROM s_catalog_returns s
LEFT OUTER JOIN date_dim d ON (s.cret_return_date = d.d_date)
LEFT OUTER JOIN time_dim t ON (hour(s.cret_return_time) = t.t_hour AND minute(s.cret_return_time) = t.t_minute AND second(s.cret_return_time) = t.t_second)
LEFT OUTER JOIN item i ON (s.cret_item_id = i.i_item_id)
LEFT OUTER JOIN customer c1 ON (s.cret_return_customer_id = c1.c_customer_id)
LEFT OUTER JOIN customer c2 ON (s.cret_refund_customer_id = c2.c_customer_id)
LEFT OUTER JOIN reason r ON (s.cret_reason_id = r.r_reason_id)
LEFT OUTER JOIN call_center cc ON (s.cret_call_center_id = cc.cc_call_center_id)
LEFT OUTER JOIN catalog_page cp ON (s.cret_catalog_page_id = cp.cp_catalog_page_id)
LEFT OUTER JOIN ship_mode sm ON (s.cret_shipmode_id = sm.sm_ship_mode_id)
LEFT OUTER JOIN warehouse w ON (s.cret_warehouse_id = w.w_warehouse_id)
WHERE i_rec_end_date IS NULL AND cc_rec_end_date IS NULL;

INSERT INTO TABLE catalog_returns SELECT * FROM crv;
DROP VIEW crv;

