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
  drop table if exists web_returns_tmp;
  create table web_returns_tmp like web_returns;
  
  insert overwrite table web_returns_tmp select * from web_returns wr left semi join (select * from web_sales ws left semi join date_dim d on ws.ws_sold_date_sk=d.d_date_sk and d.d_date not between '$date1_1' and '$date2_1' and d.d_date not between '$date1_2' and '$date2_2' and d.d_date not between '$date1_3' and '$date2_3') x on x.ws_item_sk=wr.wr_item_sk and x.ws_order_number=wr.wr_order_number;
  
  drop table web_returns;
  alter table web_returns_tmp rename to web_returns;
  
  drop table if exists web_sales_tmp;
  create table web_sales_tmp like web_sales;
  
  insert overwrite table web_sales_tmp select * from web_sales ws left semi join date_dim d on ws.ws_sold_date_sk=d.d_date_sk and d.d_date not between '$date1_1' and '$date2_1' and d.d_date not between '$date1_2' and '$date2_2' and d.d_date not between '$date1_3' and '$date2_3';
  
  drop table web_sales;
  alter table web_sales_tmp rename to web_sales;
