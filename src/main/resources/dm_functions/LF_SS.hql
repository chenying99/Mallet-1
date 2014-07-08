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
DROP VIEW IF EXISTS ssv;
CREATE VIEW ssv AS
SELECT 
  d_date_sk ss_sold_date_sk,
  t_time_sk ss_sold_time_sk,
  i_item_sk ss_item_sk,
  c_customer_sk ss_customer_sk,
  c_current_cdemo_sk ss_cdemo_sk,
  c_current_hdemo_sk ss_hdemo_sk,
  c_current_addr_sk ss_addr_sk,
  s_store_sk ss_store_sk,
  p_promo_sk ss_promo_sk,
  purc_purchase_id ss_ticket_number,
  plin_quantity ss_quantity,
  i_wholesale_cost ss_wholesale_cost,
  i_current_price ss_list_price,
  plin_sale_price ss_sales_price,
  (i_current_price-plin_sale_price)*plin_quantity ss_ext_discount_amt,
  plin_sale_price*plin_quantity ss_ext_sales_price,
  i_wholesale_cost*plin_quantity ss_ext_wholesale_cost,
  i_current_price*plin_quantity ss_ext_list_price,
  i_current_price*s_tax_precentage ss_ext_tax,
  plin_coupon_amt ss_coupon_amt,
  (plin_sale_price*plin_quantity)-plin_coupon_amt ss_net_paid,
  ((plin_sale_price*plin_quantity)-plin_coupon_amt)*(1+s_tax_precentage) ss_net_paid_inc_tax,
  ((plin_sale_price*plin_quantity)-plin_coupon_amt)-(plin_quantity*i_wholesale_cost) ss_net_profit
FROM s_purchase sp
LEFT OUTER JOIN customer c on (sp.purc_customer_id=c.c_customer_id)
LEFT OUTER JOIN store s on (sp.purc_store_id=s.s_store_id)
LEFT OUTER JOIN date_dim d on (sp.purc_purchase_date=d.d_date)
LEFT OUTER JOIN time_dim t on (sp.purc_purchase_time=t.t_time)
LEFT OUTER JOIN s_purchase_lineitem spl on (sp.purc_purchase_id=spl.plin_purchase_id)
LEFT OUTER JOIN promotion p on (spl.plin_promotion_id=p.p_promo_id)
LEFT OUTER JOIN item i on (spl.plin_item_id=i.i_item_id)
WHERE
  purc_purchase_id=plin_purchase_id AND i_rec_end_date is NULL AND s_rec_end_date is NULL;

INSERT INTO TABLE store_sales SELECT * FROM ssv;
DROP VIEW ssv;
