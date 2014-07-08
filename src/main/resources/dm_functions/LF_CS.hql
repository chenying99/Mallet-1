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
DROP VIEW IF EXISTS csv;
CREATE VIEW csv AS
SELECT 
  d1.d_date_sk cs_sold_date_sk,
  t_time_sk cs_sold_time_sk,
  d2.d_date_sk cs_ship_date_sk,
  c1.c_customer_sk cs_bill_customer_sk,
  c1.c_current_cdemo_sk cs_bill_cdemo_sk,
  c1.c_current_hdemo_sk cs_bill_hdemo_sk,
  c1.c_current_addr_sk cs_bill_addr_sk,
  c2.c_customer_sk cs_ship_customer_sk,
  c2.c_current_cdemo_sk cs_ship_cdemo_sk,
  c2.c_current_hdemo_sk cs_ship_hdemo_sk,
  c2.c_current_addr_sk cs_ship_addr_sk,
  cc_call_center_sk cs_call_center_sk,
  cp_catalog_page_sk cs_catalog_page_sk,
  sm_ship_mode_sk cs_ship_mode_sk,
  w_warehouse_sk cs_warehouse_sk,
  i_item_sk cs_item_sk,
  p_promo_sk cs_promo_sk,
  cord_order_id cs_order_number,
  clin_quantity cs_quantity,
  i_wholesale_cost cs_wholesale_cost,
  i_current_price cs_list_price,
  clin_sales_price cs_sales_price,
  (i_current_price-clin_sales_price)*clin_quantity cs_ext_discount_amt,
  clin_sales_price*clin_quantity cs_ext_sales_price,
  i_wholesale_cost*clin_quantity cs_ext_wholesale_cost,
  i_current_price*clin_quantity cs_ext_list_price,
  i_current_price*cc_tax_percentage cs_ext_tax,
  clin_coupon_amt cs_coupon_amt,
  clin_ship_cost*clin_quantity cs_ext_ship_cost,
  (clin_sales_price*clin_quantity)-clin_coupon_amt cs_net_paid,
  ((clin_sales_price*clin_quantity)-clin_coupon_amt)*(1+cc_tax_percentage) cs_net_paid_inc_tax,
  (clin_sales_price*clin_quantity)-clin_coupon_amt+(clin_ship_cost*clin_quantity) cs_net_paid_inc_ship,
  (clin_sales_price*clin_quantity)-clin_coupon_amt+(clin_ship_cost*clin_quantity)+i_current_price*cc_tax_percentage cs_net_paid_inc_ship_tax,
  ((clin_sales_price*clin_quantity)-clin_coupon_amt)-(clin_quantity*i_wholesale_cost) cs_net_profit
FROM s_catalog_order sco
  LEFT OUTER JOIN date_dim d1 ON (sco.cord_order_date=d1.d_date)
  LEFT OUTER JOIN time_dim t ON (sco.cord_order_time=t.t_time)
  LEFT OUTER JOIN customer c1 ON (sco.cord_bill_customer_id=c1.c_customer_id)
  LEFT OUTER JOIN customer c2 ON (sco.cord_ship_customer_id=c2.c_customer_id)
  LEFT OUTER JOIN call_center cc ON (sco.cord_call_center_id=cc.cc_call_center_id AND cc.cc_rec_end_date IS NULL)
  LEFT OUTER JOIN ship_mode sm ON (sco.cord_ship_mode_id=sm.sm_ship_mode_id)
  JOIN s_catalog_order_lineitem scol ON (sco.cord_order_id=scol.clin_order_id)
  LEFT OUTER JOIN date_dim d2 ON (scol.clin_ship_date=d2.d_date)
  LEFT OUTER JOIN catalog_page cp ON (scol.clin_catalog_page_number=cp.cp_catalog_page_number AND scol.clin_catalog_number=cp.cp_catalog_number)
  LEFT OUTER JOIN warehouse w ON (scol.clin_warehouse_id=w.w_warehouse_id)
  LEFT OUTER JOIN item i ON (scol.clin_item_id=i.i_item_id AND i.i_rec_end_date IS NULL)
  LEFT OUTER JOIN promotion p ON (scol.clin_promotion_id=p.p_promo_id);
  
INSERT INTO TABLE catalog_sales SELECT * FROM csv;
DROP VIEW csv;
