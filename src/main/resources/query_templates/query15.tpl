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
[COMMENT] QUERY_ID=15;

define YEAR=random(1998,2002,uniform);
 define QOY=random(1,2,uniform);
 define _LIMIT=100;
 
select  ca_zip
       ,sum(cs_sales_price)
 from catalog_sales cs join customer c
        on cs.cs_bill_customer_sk = c.c_customer_sk
      join customer_address ca
        on c.c_current_addr_sk = ca.ca_address_sk
      join date_dim d
        on cs.cs_sold_date_sk = d.d_date_sk and d.d_qoy = [QOY] and d.d_year = [YEAR]
 where 
          substr(ca_zip,1,5) in ('85669', '86197','88274','83405','86475',
                                   '85392', '85460', '80348', '81792')
          or ca_state in ('CA','WA','GA')
          or cs_sales_price > 500
 group by ca_zip
 order by ca_zip
 [_LIMITC];
