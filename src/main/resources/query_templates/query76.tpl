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

DEFINE COMMENT = "-- ";
[COMMENT] QUERY_ID=76;

define NULLCOLCS=text({"cs_bill_customer_sk",1},{"cs_bill_hdemo_sk",1},{"cs_bill_addr_sk",1},{"cs_ship_customer_sk",1},{"cs_ship_cdemo_sk",1},{"cs_ship_hdemo_sk",1},{"cs_ship_addr_sk",1},{"cs_ship_mode_sk",1},{"cs_warehouse_sk",1},{"cs_promo_sk",1});
define NULLCOLSS= text({"ss_customer_sk",1},{"ss_cdemo_sk",1},{"ss_hdemo_sk",1},{"ss_addr_sk",1},{"ss_store_sk",1},{"ss_promo_sk",1});
define NULLCOLWS=text({"ws_bill_customer_sk",1},{"ws_bill_hdemo_sk",1},{"ws_bill_addr_sk",1},{"ws_ship_customer_sk",1},{"ws_ship_cdemo_sk",1},{"ws_ship_hdemo_sk",1},{"ws_ship_addr_sk",1},{"ws_web_page_sk",1},{"ws_web_site_sk",1},{"ws_ship_mode_sk",1},{"ws_warehouse_sk",1},{"ws_promo_sk",1});
define _LIMIT=100;

select  channel, col_name, d_year, d_qoy, i_category, COUNT(*) sales_cnt, SUM(ext_sales_price) sales_amt FROM (
        SELECT 'store' as channel, '[NULLCOLSS]' col_name, d_year, d_qoy, i_category, ss_ext_sales_price ext_sales_price
         FROM store_sales ss join item i
                on ss.ss_item_sk=i.i_item_sk
                and ss.[NULLCOLSS] IS NULL
              join date_dim d
                on ss.ss_sold_date_sk=d.d_date_sk
        UNION ALL
        SELECT 'web' as channel, '[NULLCOLWS]' col_name, d_year, d_qoy, i_category, ws_ext_sales_price ext_sales_price
         FROM web_sales ws join item i
                on ws.ws_item_sk=i.i_item_sk
                and ws.[NULLCOLWS] IS NULL
              join date_dim d
                on ws.ws_sold_date_sk=d.d_date_sk
        UNION ALL
        SELECT 'catalog' as channel, '[NULLCOLCS]' col_name, d_year, d_qoy, i_category, cs_ext_sales_price ext_sales_price
         FROM catalog_sales cs join item i
                on cs.cs_item_sk=i.i_item_sk
                and cs.[NULLCOLCS] IS NULL
              join date_dim d
                on cs.cs_sold_date_sk=d.d_date_sk) foo
GROUP BY channel, col_name, d_year, d_qoy, i_category
ORDER BY channel, col_name, d_year, d_qoy, i_category
[_LIMITC];

 
 

