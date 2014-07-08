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

USE mallet_db;

DROP VIEW IF EXISTS iv;
CREATE VIEW iv AS
  select
    d_date_sk as inv_date_sk,
    i_item_sk as inv_item_sk,
    w_warehouse_sk as inv_warehouse_sk,
    invn_qty_on_hand as inv_quantity_on_hand
  from s_inventory si
    left outer join warehouse w on si.invn_warehouse_id=w.w_warehouse_id
    left outer join item i on si.invn_item_id=i.i_item_id and i.i_rec_end_date is null
    left outer join date_dim d on si.invn_date=d.d_date;
    
INSERT INTO TABLE inventory SELECT * FROM iv;
DROP VIEW iv;
