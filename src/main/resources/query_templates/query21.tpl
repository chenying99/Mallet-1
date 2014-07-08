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
[COMMENT] QUERY_ID=21;


-- 0 -> 0L
-- date is treated as string
 define YEAR=random(1998,2002,uniform);
 define SALES_DATE=date([YEAR]+"-01-31",[YEAR]+"-7-01",sales);
 define _LIMIT=100;

select  *
 from(select w_warehouse_name
            ,i_item_id
            ,sum(case when d_date < '[SALES_DATE]'
                    then inv_quantity_on_hand 
                      else 0L end) as inv_before
            ,sum(case when d_date >= '[SALES_DATE]'
                      then inv_quantity_on_hand 
                      else 0L end) as inv_after
   from 
        inventory inv join warehouse w
          on inv.inv_warehouse_sk   = w.w_warehouse_sk
        join item i
          on i.i_current_price between 0.99 and 1.49
          and i.i_item_sk          = inv.inv_item_sk
        join date_dim d
          on inv.inv_date_sk    = d.d_date_sk
          and d.d_date between date_sub('[SALES_DATE]',30)
                    and date_add('[SALES_DATE]',30)
   group by w_warehouse_name, i_item_id) x
 where (case when inv_before > 0 
             then inv_after / inv_before 
             else null
             end) between 2.0/3.0 and 3.0/2.0
 order by w_warehouse_name
         ,i_item_id
 [_LIMITC];
