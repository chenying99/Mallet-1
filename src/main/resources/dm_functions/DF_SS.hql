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

  use mallet_db;
  drop table if exists store_returns_tmp;
  create table store_returns_tmp like store_returns;
  
  insert overwrite table store_returns_tmp select * from store_returns sr left semi join (select * from store_sales ss left semi join date_dim d on ss.ss_sold_date_sk=d.d_date_sk and d.d_date not between '$date1_1' and '$date2_1' and d.d_date not between '$date1_2' and '$date2_2' and d.d_date not between '$date1_3' and '$date2_3') x on x.ss_item_sk=sr.sr_item_sk and x.ss_ticket_number=sr.sr_ticket_number;
  
  drop table store_returns; 
  alter table store_returns_tmp rename to store_returns;
  
  drop table if exists store_sales_tmp;
  create table store_sales_tmp like store_sales;
  
  insert overwrite table store_sales_tmp select * from store_sales ss left semi join date_dim d on ss.ss_sold_date_sk=d.d_date_sk and d.d_date not between '$date1_1' and '$date2_1' and d.d_date not between '$date1_2' and '$date2_2' and d.d_date not between '$date1_3' and '$date2_3';
  
  drop table store_sales;
  alter table store_sales_tmp rename to store_sales;
