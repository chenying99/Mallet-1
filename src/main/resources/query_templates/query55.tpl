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
[COMMENT] QUERY_ID=55;

 define YEAR= random(1998, 2002, uniform);
 define MONTH=random(11,12,uniform);
 define MANAGER=random(1,100,uniform); 
 define _LIMIT=100;
 
select  i_brand_id brand_id, i_brand brand,
    sum(ss_ext_sales_price) ext_price
 from 
      date_dim d join store_sales ss
        on d.d_date_sk = ss.ss_sold_date_sk
      join item i
        on ss.ss_item_sk = i.i_item_sk
 where
    i_manager_id=[MANAGER]
    and d_moy=[MONTH]
    and d_year=[YEAR]
 group by i_brand, i_brand_id
 order by ext_price desc, brand_id
[_LIMITC] ;


