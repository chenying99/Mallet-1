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
[COMMENT] QUERY_ID=98;

Define YEAR=random(1998,2002,uniform);
Define SDATE=date([YEAR]+"-01-01",[YEAR]+"-07-01",sales);
Define CATEGORY=ulist(dist(categories,1,1),3);

select i_item_desc 
      ,i_category 
      ,i_class 
      ,i_item_id
      ,i_current_price
      ,itemrevenue 
      ,itemrevenue*100/sum(itemrevenue) over
          (partition by i_class) as revenueratio
from (  
    select i_item_desc 
          ,i_category 
          ,i_class 
          ,i_item_id
          ,i_current_price
          ,sum(ss_ext_sales_price) as itemrevenue 
    from    
        store_sales ss join item i
          on ss.ss_item_sk = i.i_item_sk
          and i.i_category in ('[CATEGORY.1]', '[CATEGORY.2]', '[CATEGORY.3]')
        join date_dim d
          on ss.ss_sold_date_sk = d.d_date_sk
          and d.d_date between '[SDATE]'
                    and date_add('[SDATE]', 30)
    group by 
        i_item_id
            ,i_item_desc 
            ,i_category
            ,i_class
            ,i_current_price ) x
order by 
    i_category
        ,i_class
        ,i_item_id
        ,i_item_desc
        ,revenueratio;


