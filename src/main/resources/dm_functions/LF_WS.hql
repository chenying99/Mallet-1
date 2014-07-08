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
DROP VIEW IF EXISTS wsv;
CREATE VIEW wsv AS
SELECT
  d1.d_date_sk ws_sold_date_sk, 
        t_time_sk ws_sold_time_sk, 
        d2.d_date_sk ws_ship_date_sk,
        i_item_sk ws_item_sk, 
        c1.c_customer_sk ws_bill_customer_sk, 
        c1.c_current_cdemo_sk ws_bill_cdemo_sk, 
        c1.c_current_hdemo_sk ws_bill_hdemo_sk,
        c1.c_current_addr_sk ws_bill_addr_sk,
        c2.c_customer_sk ws_ship_customer_sk,
        c2.c_current_cdemo_sk ws_ship_cdemo_sk,
        c2.c_current_hdemo_sk ws_ship_hdemo_sk,
        c2.c_current_addr_sk ws_ship_addr_sk,
        wp_web_page_sk ws_web_page_sk,
        web_site_sk ws_web_site_sk,
        sm_ship_mode_sk ws_ship_mode_sk,
        w_warehouse_sk ws_warehouse_sk,
        p_promo_sk ws_promo_sk,
        word_order_id ws_order_number, 
        wlin_quantity ws_quantity, 
        i_wholesale_cost ws_wholesale_cost, 
        i_current_price ws_list_price,
        wlin_sales_price ws_sales_price,
        (i_current_price-wlin_sales_price)*wlin_quantity ws_ext_discount_amt,
        wlin_sales_price * wlin_quantity ws_ext_sales_price,
        i_wholesale_cost * wlin_quantity ws_ext_wholesale_cost, 
        i_current_price * wlin_quantity ws_ext_list_price, 
        i_current_price * web_tax_percentage ws_ext_tax,  
        wlin_coupon_amt ws_coupon_amt,
        wlin_ship_cost * wlin_quantity WS_EXT_SHIP_COST,
        (wlin_sales_price * wlin_quantity)-wlin_coupon_amt ws_net_paid,
        ((wlin_sales_price * wlin_quantity)-wlin_coupon_amt)*(1+web_tax_percentage) ws_net_paid_inc_tax,
        ((wlin_sales_price * wlin_quantity)-wlin_coupon_amt)-(wlin_quantity*i_wholesale_cost) WS_NET_PAID_INC_SHIP,
        (wlin_sales_price * wlin_quantity)-wlin_coupon_amt + (wlin_ship_cost * wlin_quantity)
        + i_current_price * web_tax_percentage WS_NET_PAID_INC_SHIP_TAX,
        ((wlin_sales_price * wlin_quantity)-wlin_coupon_amt)-(i_wholesale_cost * wlin_quantity) WS_NET_PROFIT
FROM    s_web_order swo1
LEFT OUTER JOIN date_dim d1 ON (swo1.word_order_date = d1.d_date)
LEFT OUTER JOIN time_dim t1 ON (swo1.word_order_time = t1.t_time)
LEFT OUTER JOIN customer c1 ON (swo1.word_bill_customer_id = c1.c_customer_id)
LEFT OUTER JOIN customer c2 ON (swo1.word_ship_customer_id = c2.c_customer_id)
LEFT OUTER JOIN web_site ws1 ON (swo1.word_web_site_id = ws1.web_site_id AND ws1.web_rec_end_date IS NULL)
LEFT OUTER JOIN ship_mode sm1 ON (swo1.word_ship_mode_id = sm1.sm_ship_mode_id)
JOIN s_web_order_lineitem swol1 ON (swo1.word_order_id = swol1.wlin_order_id)
LEFT OUTER JOIN date_dim d2 ON (swol1.wlin_ship_date = d2.d_date)
LEFT OUTER JOIN item i1 ON (swol1.wlin_item_id = i1.i_item_id AND i1.i_rec_end_date IS NULL)
LEFT OUTER JOIN web_page wp1 ON (swol1.wlin_web_page_id = wp1.wp_web_page_id AND wp1.wp_rec_end_date IS NULL)
LEFT OUTER JOIN warehouse w1 ON (swol1.wlin_warehouse_id = w1.w_warehouse_id)
LEFT OUTER JOIN promotion p1 ON (swol1.wlin_promotion_id = p1.p_promo_id);

INSERT INTO TABLE web_sales SELECT * FROM wsv;
DROP VIEW wsv;
