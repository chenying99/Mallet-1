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
[COMMENT] QUERY_ID=92;

Define IMID  = random(1,1000,uniform);
Define YEAR  = random(1998,2002,uniform);
Define WSDATE = date([YEAR]+"-01-01",[YEAR]+"-04-01",sales);
define _LIMIT=100;

select  
   sum(ws_ext_discount_amt)  as sum_amt
from (
    select i_item_sk, ws_ext_discount_amt
    from 
        web_sales ws join item i
          on i.i_manufact_id = [IMID]
          and i.i_item_sk = ws.ws_item_sk 
        join date_dim d
          on d.d_date between '[WSDATE]' and
                             date_add('[WSDATE]',90)
          and d.d_date_sk = ws.ws_sold_date_sk)
  tmp1 join ( 
         SELECT 
            ws_item_sk, 1.3 * avg(ws_ext_discount_amt) as avg_ws_ext_discount_amt
         FROM 
            web_sales ws join date_dim d
              on d.d_date between '[WSDATE]' and
                             date_add('[WSDATE]',90)
              and d.d_date_sk = ws.ws_sold_date_sk
         GROUP BY ws_item_sk
      ) tmp2
    on tmp1.i_item_sk = tmp2.ws_item_sk
where ws_ext_discount_amt > avg_ws_ext_discount_amt
order by sum_amt
[_LIMITC]; 

