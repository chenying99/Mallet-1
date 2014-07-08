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
[COMMENT] QUERY_ID=91;

Define YEAR = random(1998,2002, uniform);
Define MONTH = random(11,12,uniform);
Define BUY_POTENTIAL = text({"1001-5000",1},{">10000",1},{"501-1000",1},{"0-500",1},{"Unknown",1},{"5001-10000",1});
Define GMT = text({"-6",1},{"-7",1});

select  
        cc_call_center_id Call_Center,
        cc_name Call_Center_Name,
        cc_manager Manager,
        sum(cr_net_loss) Returns_Loss
from
        call_center cc join catalog_returns cr
          on cr.cr_call_center_sk       = cc.cc_call_center_sk
        join date_dim d
          on cr.cr_returned_date_sk     = d.d_date_sk
          and d.d_year                  = [YEAR]
          and d.d_moy                   = [MONTH]
        join customer c
          on cr.cr_returning_customer_sk= c.c_customer_sk
        join customer_address ca
          on ca.ca_address_sk           = c.c_current_addr_sk
          and ca.ca_gmt_offset          = [GMT]
        join customer_demographics cd
          on cd.cd_demo_sk              = c.c_current_cdemo_sk
        join household_demographics hd
          on hd.hd_demo_sk              = c.c_current_hdemo_sk
          and hd.hd_buy_potential like '[BUY_POTENTIAL]%'
where
     ( (cd_marital_status       = 'M' and cd_education_status     = 'Unknown')
        or(cd_marital_status       = 'W' and cd_education_status     = 'Advanced Degree'))
group by cc_call_center_id,cc_name,cc_manager,cd_marital_status,cd_education_status
order by Returns_Loss desc;


