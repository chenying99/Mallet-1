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
  drop table if exists catalog_returns_tmp;
  create table catalog_returns_tmp like catalog_returns;
  
  insert overwrite table catalog_returns_tmp select * from catalog_returns cr left semi join (select * from catalog_sales cs left semi join date_dim d on cs.cs_sold_date_sk=d.d_date_sk and d.d_date not between '$date1_1' and '$date2_1' and d.d_date not between '$date1_2' and '$date2_2' and d.d_date not between '$date1_3' and '$date2_3') x on x.cs_item_sk=cr.cr_item_sk and x.cs_order_number=cr.cr_order_number;
  
  drop table catalog_returns;
  alter table catalog_returns_tmp rename to catalog_returns;
  
  drop table if exists catalog_sales_tmp;
  create table catalog_sales_tmp like catalog_sales;
  
  insert overwrite table catalog_sales_tmp select * from catalog_sales cs left semi join date_dim d on cs.cs_sold_date_sk=d.d_date_sk and d.d_date not between '$date1_1' and '$date2_1' and d.d_date not between '$date1_2' and '$date2_2' and d.d_date not between '$date1_3' and '$date2_3';
  
  drop table catalog_sales;
  alter table catalog_sales_tmp rename to catalog_sales;
