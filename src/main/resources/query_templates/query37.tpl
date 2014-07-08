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
[COMMENT] QUERY_ID=37;

 define YEAR=random(1998,2002,uniform);
 define INVDATE=date([YEAR]+"-01-01",[YEAR]+"-07-24",sales);
 define MANUFACT_ID=ulist(random(667,1000,uniform),4);
 define PRICE=random(10,70,uniform);
 define _LIMIT=100;
  
 select  i_item_id
       ,i_item_desc
       ,i_current_price
 from
    item i join inventory inv
      on i.i_current_price between [PRICE] and [PRICE] + 30
      and inv.inv_item_sk = i.i_item_sk
      and i.i_manufact_id in ([MANUFACT_ID.1],[MANUFACT_ID.2],[MANUFACT_ID.3],[MANUFACT_ID.4])
      and inv.inv_quantity_on_hand between 100 and 500
    join date_dim d
      on d.d_date_sk=inv.inv_date_sk
      and d.d_date between '[INVDATE]' and date_add('[INVDATE]',60)
    join catalog_sales cs
      on cs.cs_item_sk = i.i_item_sk
 group by i_item_id,i_item_desc,i_current_price
 order by i_item_id
 [_LIMITC];
 

