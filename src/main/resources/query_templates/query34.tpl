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
[COMMENT] QUERY_ID=34;

 define BPONE= text({"1001-5000",1},{">10000",1},{"501-1000",1});
 define BPTWO= text({"0-500",1},{"unknown",1},{"5001-10000",1});
 define YEAR= random(1998, 2000, uniform);
 define COUNTYNUMBER=ulist(random(1, rowcount("active_counties", "store"), uniform), 8);
 define COUNTY_A=distmember(fips_county, [COUNTYNUMBER.1], 2);
 define COUNTY_B=distmember(fips_county, [COUNTYNUMBER.2], 2);
 define COUNTY_C=distmember(fips_county, [COUNTYNUMBER.3], 2);
 define COUNTY_D=distmember(fips_county, [COUNTYNUMBER.4], 2);
 define COUNTY_E=distmember(fips_county, [COUNTYNUMBER.5], 2);
 define COUNTY_F=distmember(fips_county, [COUNTYNUMBER.6], 2);
 define COUNTY_G=distmember(fips_county, [COUNTYNUMBER.7], 2);
 define COUNTY_H=distmember(fips_county, [COUNTYNUMBER.8], 2);
 
 
 -- TODO:
-- FAILED: Error in semantic analysis: TOK_NULL encountered with 0 children
-- related to the CASE When operator

select c_last_name
       ,c_first_name
       ,c_salutation
       ,c_preferred_cust_flag
       ,ss_ticket_number
       ,cnt from
   (select ss_ticket_number
          ,ss_customer_sk
          ,count(*) cnt
    from
      store_sales join date_dim
        on store_sales.ss_sold_date_sk = date_dim.d_date_sk
        and date_dim.d_year in ([YEAR],[YEAR]+1,[YEAR]+2)
      join store
        on store_sales.ss_store_sk = store.s_store_sk
        and store.s_county in ('[COUNTY_A]','[COUNTY_B]','[COUNTY_C]','[COUNTY_D]',
                           '[COUNTY_E]','[COUNTY_F]','[COUNTY_G]','[COUNTY_H]')
      join household_demographics
        on store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
        and household_demographics.hd_vehicle_count > 0
        and (case when household_demographics.hd_vehicle_count > 0 
            then household_demographics.hd_dep_count/ household_demographics.hd_vehicle_count 
            else 0.0 
            end)  > 1.2
    where
        (date_dim.d_dom between 1 and 3 or date_dim.d_dom between 25 and 28)
        and (household_demographics.hd_buy_potential = '[BPONE]' or
             household_demographics.hd_buy_potential = '[BPTWO]')
    group by ss_ticket_number,ss_customer_sk) dn
    join customer c
      on dn.ss_customer_sk = c.c_customer_sk and dn.cnt between 15 and 20   
    order by c_last_name,c_first_name,c_salutation,c_preferred_cust_flag desc;

