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
[COMMENT] QUERY_ID=45;

 define GBOBC= text({"ca_city",1},{"ca_county",1},{"ca_state",1});
 define YEAR=random(1998,2002,uniform);
 define QOY=random(1,2,uniform);
 define _LIMIT=100;
 
select  ca_zip, [GBOBC], sum(ws_sales_price)
 from 
    web_sales ws join customer c
      on ws.ws_bill_customer_sk = c.c_customer_sk
    join customer_address ca
      on c.c_current_addr_sk = ca.ca_address_sk 
    join date_dim d
      on ws.ws_sold_date_sk = d.d_date_sk
      and d.d_qoy = [QOY]
      and d.d_year = [YEAR]
    join item i
      on ws.ws_item_sk = i.i_item_sk 
    join (select collect_set(i_item_id) as item_set
                             from item
                             where i_item_sk in (2, 3, 5, 7, 11, 13, 17, 19, 23, 29)
                             ) tmp
 where 
        ( substr(ca_zip,1,5) in ('85669', '86197','88274','83405','86475', '85392', '85460', '80348', '81792')
          or 
          array_contains(item_set, i_item_id)
        )
 group by ca_zip, [GBOBC]
 order by ca_zip, [GBOBC]
 [_LIMITC];
