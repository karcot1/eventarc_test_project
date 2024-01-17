
//  # Copyright 2020 Google LLC
//  #
//  # Licensed under the Apache License, Version 2.0 (the "License");
//  # you may not use this file except in compliance with the License.
//  # You may obtain a copy of the License at
//  #
//  #      http://www.apache.org/licenses/LICENSE-2.0
//  #
//  # Unless required by applicable law or agreed to in writing, software
//  # distributed under the License is distributed on an "AS IS" BASIS,
//  # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  # See the License for the specific language governing permissions and
//  # limitations under the License.

//------------------------------------------------------------------------------------------------------------------------------------------------------------
//                              Reusable SCD2 Backfill
// The below code executes in two parts
//     1. Create Child SCD tables from start_date to start_date + no_of_days until the last date is reached. 
//         The child tables executes parallely with the maximum no of parallel job being defined with concurrentQueryLimit in the parameters
//     2. Update the final table to get the right value of min_eff_date_source from the new data and merge the child table and the final table
//        There is also an insert that happens after the merge completes to address a special scenario.
//
//------------------------------------------------------------------------------------------------------------------------------------------------------------

// ---------------------------------------------------------------------------------------------------
// Function getDaysArray - Input parameter -> 3  - Output -> Array
// Parameters - Start Date, End Date and No Of Days
// The function returns an array of dates between start date and end date portioned based on the no of days passed on through variables
// Eg - getDaysArray(new Date("2021-09-10"),new Date("2021-12-10"),new Number(31)) -
// Output [ '2021-09-10:2021-10-11',
//          '2021-10-11:2021-11-11',
//          '2021-11-11:2021-12-10' ]
// ---------------------------------------------------------------------------------------------------


// ---------------------------------------------------------------------------------------------------
// Declaring and initaizing a counter to maintain the number of child tables generated 
// ---------------------------------------------------------------------------------------------------
  constructor() 
  {
      this.count = 0;
  }


// ---------------------------------------------------------------------------------------------------
// Start Creating and processing of Child Tables
// ---------------------------------------------------------------------------------------------------

//   var daylist = getDaysArray(new Date(dataform.projectConfig.vars.from_date),new Date(dataform.projectConfig.vars.to_date),new Number(dataform.projectConfig.vars.no_of_days));
  var daylist_str = dataform.projectConfig.vars.daylist;
  daylist_str = daylist_str.replace(/'/g, '"'); //replacing all ' with "
  var daylist = JSON.parse(daylist_str);

  daylist.map(pair => pair.split(":")).forEach(daylist => {
  console.log("Daylist - " +daylist);
  operate("scd2_" + this.count,{
      hasOutput: true
    }).queries(`


  DECLARE src_unique STRING;
  DECLARE src_non_unique STRING;
  DECLARE final_target_unique STRING;
  DECLARE src_non_unique_proc STRING;

-- src_unique returns a comma separated string of the unique keys of the source table from the metadata table 'table_to_unique'
  SET src_unique = (
  WITH selected_columns as (
  SELECT primary_key_column_name  
      FROM ${dataform.projectConfig.defaultSchema}.table_to_unique
      WHERE 
      table_name =  "${dataform.projectConfig.vars.src_table}"  and table_catalog = "${dataform.projectConfig.vars.src_database}" and table_schema = "${dataform.projectConfig.vars.src_schema}"
  )
  SELECT STRING_AGG(primary_key_column_name) AS columns FROM selected_columns
  );

  -- final_target_unique returns a comma separated string of the unique keys of the final target table from the metadata table 'table_to_unique'
  SET final_target_unique = (
  WITH selected_columns as (
  SELECT primary_key_column_name  
      FROM ${dataform.projectConfig.defaultSchema}.table_to_unique
      WHERE 
      table_name =  "${dataform.projectConfig.vars.target_table}"  and table_catalog = "${dataform.projectConfig.vars.target_database}" and table_schema = "${dataform.projectConfig.vars.target_schema}"
  )
  SELECT STRING_AGG(primary_key_column_name) AS columns FROM selected_columns
  );
  
  -- src_non_unique returns a comma separated string of the non unique keys of the source  table from the metadata table 'INFORMATION_SCHEMA.COLUMNS'
  SET src_non_unique = (
  WITH selected_columns as (
  SELECT column_name 
      FROM ${dataform.projectConfig.vars.src_database}.${dataform.projectConfig.vars.src_schema}.INFORMATION_SCHEMA.COLUMNS
      WHERE 
      table_name =  "${dataform.projectConfig.vars.src_table}" and column_name not in (SELECT primary_key_column_name  
      FROM ${dataform.projectConfig.defaultSchema}.table_to_unique
      WHERE 
      table_name =  "${dataform.projectConfig.vars.src_table}"  and table_catalog = "${dataform.projectConfig.vars.src_database}" and table_schema = "${dataform.projectConfig.vars.src_schema}"
  ) and column_name <> "${dataform.projectConfig.vars.timestampfield}" 
  )
  SELECT STRING_AGG(column_name) AS columns FROM selected_columns
  );
  
  -- src_non_unique_proc returns a comma separated coalesced and concated string of the non unique keys of the source table from the metadata table 'INFORMATION_SCHEMA.COLUMNS'
  SET src_non_unique_proc = (
  WITH selected_columns as (
  SELECT concat(' coalesce(cast(',column_name,' as STRING),"NA")') as clean_data
      FROM ${dataform.projectConfig.vars.src_database}.${dataform.projectConfig.vars.src_schema}.INFORMATION_SCHEMA.COLUMNS
      WHERE 
      table_name =  "${dataform.projectConfig.vars.src_table}" and column_name not in (SELECT primary_key_column_name  
      FROM ${dataform.projectConfig.defaultSchema}.table_to_unique
      WHERE 
      table_name =  "${dataform.projectConfig.vars.src_table}"  and table_catalog = "${dataform.projectConfig.vars.src_database}" and table_schema = "${dataform.projectConfig.vars.src_schema}"
  ) and column_name <> "${dataform.projectConfig.vars.timestampfield}" 
  )
  SELECT STRING_AGG(clean_data) AS columns FROM selected_columns
  );


  -- ----------------------------------------------------------------------------------------------------------------------------
  -- The SCD2 Child tables are build around two concepts Row Numbers and Groups. 
  -- Row numbers are calculated based on the Primary key and the changing fields / non unique fields 
  -- Groups are subtractions of row_number of primary key to the row_number of primary key and changing fields / non unique fields. 
  -- Groups are implemented to make sure if all of the changing field go back to the same value as it was n days back, it still gives us the right date
  -- Group are set to -1 if there is a difference between dates. For instance, if we stop getting data from 2022-10-10 till 2022-12-10, the date of 2022-10-09 will have the results of grouped multiplied by (-1)
  -- Another flag important for this execution is the readded_data_flag. It sets the value 'Y' to the min(eff_date) of a primary key where the min(eff_date) over primary key <> min(eff_date) child table 
  -- ----------------------------------------------------------------------------------------------------------------------------

  EXECUTE IMMEDIATE format(
    """
  CREATE OR REPLACE TABLE ${dataform.projectConfig.vars.target_schema}.${dataform.projectConfig.vars.target_table}_child_delete_${this.count}
  as (Select %s,
    %s, ${dataform.projectConfig.vars.timestampfield}, ${dataform.projectConfig.vars.start_from_column_name}
    , ${dataform.projectConfig.vars.end_at_column_name},${dataform.projectConfig.vars.target_hash_unique_col_name}
    ,  ${dataform.projectConfig.vars.target_hash_non_unique_col_name}
    ,  case when b.combined_key_in is not null then 'Y' else 'N' end as readded_data_flag
      from (select
              %s, %s, ${dataform.projectConfig.vars.timestampfield},
              row_number() OVER (PARTITION BY %s, %s, grp order by ${dataform.projectConfig.vars.timestampfield} asc) as row_num, 
              grp,
              min(${dataform.projectConfig.vars.timestampfield}) over (PARTITION BY %s, %s,grp)  as ${dataform.projectConfig.vars.start_from_column_name},
              CASE 
                WHEN grp < 0 
                  THEN ${dataform.projectConfig.vars.timestampfield} 
                WHEN max(${dataform.projectConfig.vars.timestampfield}) over (PARTITION BY %s) = (SELECT max(${dataform.projectConfig.vars.timestampfield}) from ${dataform.projectConfig.vars.src_schema}.${dataform.projectConfig.vars.src_table} where ${dataform.projectConfig.vars.timestampfield} >= cast('${daylist[0]}' as Date) and ${dataform.projectConfig.vars.timestampfield} < cast('${daylist[1]}' as Date)) or ${dataform.projectConfig.vars.timestampfield} <> max(${dataform.projectConfig.vars.timestampfield}) over (PARTITION BY %s) 
                  THEN date_add((lead(${dataform.projectConfig.vars.timestampfield}) over (partition by %s order by ${dataform.projectConfig.vars.timestampfield} asc)), INTERVAL -1 Day) 
                ELSE max(${dataform.projectConfig.vars.timestampfield}) over (PARTITION BY %s) 
                END as ${dataform.projectConfig.vars.end_at_column_name}, 
                ${dataform.projectConfig.vars.target_hash_unique_col_name},  ${dataform.projectConfig.vars.target_hash_non_unique_col_name}
            from ( 
                    select %s, %s, ${dataform.projectConfig.vars.timestampfield}, 
                    date_diff(lead(${dataform.projectConfig.vars.timestampfield}) over (partition by %s order by ${dataform.projectConfig.vars.timestampfield} asc), ${dataform.projectConfig.vars.timestampfield} ,DAY),
                    CASE WHEN date_diff(lead(${dataform.projectConfig.vars.timestampfield}) over (partition by %s order by ${dataform.projectConfig.vars.timestampfield} asc),${dataform.projectConfig.vars.timestampfield},DAY)>1 
                      THEN (row_number() over (partition by %s order by ${dataform.projectConfig.vars.timestampfield} asc) - row_number() over (PARTITION BY %s, %s order by ${dataform.projectConfig.vars.timestampfield} asc))* (-1) 
                      ELSE row_number() over (partition by %s order by ${dataform.projectConfig.vars.timestampfield} asc) -row_number() over (PARTITION BY %s,%sorder by ${dataform.projectConfig.vars.timestampfield} asc)
                      END as grp,
                      TO_BASE64(md5(IFNULL(NULLIF(UPPER(TRIM(CAST(CONCAT(%s)AS STRING))),''),'^^'))) as ${dataform.projectConfig.vars.target_hash_unique_col_name}, 
                      TO_BASE64(md5(IFNULL(NULLIF(UPPER(TRIM(CAST(CONCAT(%s)AS STRING))),''),'^^'))) as ${dataform.projectConfig.vars.target_hash_non_unique_col_name}
                    from (
                          SELECT DISTINCT * from ${dataform.projectConfig.vars.src_schema}.${dataform.projectConfig.vars.src_table} where            ${dataform.projectConfig.vars.timestampfield} >= cast('${daylist[0]}' as Date)
               and date < cast('${daylist[1]}' as Date) 
                          ) t
                    where ${dataform.projectConfig.vars.timestampfield} >= cast('${daylist[0]}' as Date) and ${dataform.projectConfig.vars.timestampfield} < cast('${daylist[1]}' as Date)
                  ) 
              ) as a 
                    left join (SELECT min(${dataform.projectConfig.vars.timestampfield}) as date_in , concat(%s) as combined_key_in from dataform.covid_staging where 
              date >= cast('${daylist[0]}' as Date)
               and ${dataform.projectConfig.vars.timestampfield} < cast('${daylist[1]}' as Date)  group by %s having min(${dataform.projectConfig.vars.timestampfield}) <> cast('${daylist[0]}' as Date) ) 
               as b 
             on concat(%s) = b.combined_key_in and 
             a.${dataform.projectConfig.vars.timestampfield} = b.date_in 
      where row_num = (
        select max(row_num) from (
          select %s, %s, ${dataform.projectConfig.vars.timestampfield}, row_number() OVER (PARTITION BY %s, %s ,grp order by ${dataform.projectConfig.vars.timestampfield} asc) as row_num,
          grp,
          min(${dataform.projectConfig.vars.timestampfield}) over (PARTITION BY %s,%s,grp) as ${dataform.projectConfig.vars.start_from_column_name},
          -- lead(date) over (partition by %s order by date asc) as exp_date,
          CASE 
            WHEN max(${dataform.projectConfig.vars.timestampfield}) over (PARTITION BY %s) = (
              SELECT max(${dataform.projectConfig.vars.timestampfield}) from ${dataform.projectConfig.vars.src_schema}.${dataform.projectConfig.vars.src_table} where ${dataform.projectConfig.vars.timestampfield} >= cast('${daylist[0]}' as Date)
              and ${dataform.projectConfig.vars.timestampfield} < cast('${daylist[1]}' as Date)) 
              or ${dataform.projectConfig.vars.timestampfield} <> max(${dataform.projectConfig.vars.timestampfield}) over (PARTITION BY %s) 
            THEN lead(${dataform.projectConfig.vars.timestampfield}) over (partition by %s order by ${dataform.projectConfig.vars.timestampfield} asc) else max(${dataform.projectConfig.vars.timestampfield}) over (PARTITION BY %s) end as ${dataform.projectConfig.vars.end_at_column_name}, 
            ${dataform.projectConfig.vars.target_hash_unique_col_name},  ${dataform.projectConfig.vars.target_hash_non_unique_col_name}
              from ( 
                select %s, %s, ${dataform.projectConfig.vars.timestampfield}, 
                case 
                when date_diff(lead(${dataform.projectConfig.vars.timestampfield}) over (partition by %s order by ${dataform.projectConfig.vars.timestampfield} asc), ${dataform.projectConfig.vars.timestampfield} ,DAY)>1 
                then (row_number() over (partition by %s order by ${dataform.projectConfig.vars.timestampfield} asc) - row_number() over (PARTITION BY %s, %s order by ${dataform.projectConfig.vars.timestampfield} asc))* (-1) 
                else row_number() over (partition by %s order by ${dataform.projectConfig.vars.timestampfield} asc) - row_number() over (PARTITION BY %s,%s order by ${dataform.projectConfig.vars.timestampfield} asc) end
                as grp,
                TO_BASE64(md5(IFNULL(NULLIF(UPPER(TRIM(CAST(CONCAT(%s)AS STRING))),''),'^^'))) as ${dataform.projectConfig.vars.target_hash_unique_col_name}, 
                TO_BASE64(md5(IFNULL(NULLIF(UPPER(TRIM(CAST(CONCAT(%s)AS STRING))),''),'^^'))) as ${dataform.projectConfig.vars.target_hash_non_unique_col_name}
                from (
                  select DISTINCT * from ${dataform.projectConfig.vars.src_schema}.${dataform.projectConfig.vars.src_table} where  ${dataform.projectConfig.vars.timestampfield} >= cast('${daylist[0]}' as Date)
               and ${dataform.projectConfig.vars.timestampfield} < cast('${daylist[1]}' as Date) 
                  ) t
              where 
              ${dataform.projectConfig.vars.timestampfield} >= cast('${daylist[0]}' as Date)
               and ${dataform.projectConfig.vars.timestampfield} < cast('${daylist[1]}' as Date)
               ) 
               )
              where
                          ${dataform.projectConfig.vars.target_hash_unique_col_name} = a.${dataform.projectConfig.vars.target_hash_unique_col_name} and
              ${dataform.projectConfig.vars.target_hash_non_unique_col_name} = a.${dataform.projectConfig.vars.target_hash_non_unique_col_name} and
              grp = a.grp
  
          )
      )  
  """,src_unique, src_non_unique, src_non_unique, src_unique, src_unique, src_non_unique_proc, src_unique, src_non_unique_proc, src_unique, src_unique, src_unique, src_unique, src_non_unique, src_unique,src_unique, src_unique, src_unique, src_unique, src_non_unique_proc, src_unique, src_unique,src_non_unique_proc,src_unique,src_non_unique_proc, src_unique, src_unique, src_unique,src_non_unique, src_unique, src_unique, src_non_unique_proc, src_unique, src_non_unique_proc, src_unique, src_unique, src_unique, src_unique,src_unique,src_non_unique, src_unique, src_unique, src_unique, src_unique, src_non_unique_proc, src_unique, src_unique, src_non_unique_proc,src_unique, src_non_unique_proc )
  `).tags(["scd2_backfill"]),
  
        ++this.count;
      
  }
  );
// ---------------------------------------------------------------------------------------------------
// End Creating and processing of Child Tables
// ---------------------------------------------------------------------------------------------------

// ---------------------------------------------------------------------------------------------------
// Start Creating and processing of Final Table
// Merge max_count - 1 child tables to the final table.
// ---------------------------------------------------------------------------------------------------

  var max_count = this.count;
  for(this.count = 0; this.count< max_count; ++this.count){
// If the counter is 0, create a new table and insert the data of Child Table 0
    if (this.count ==0)
    {
      operate("scd2_merge_"+this.count)
      .dependencies("scd2_" + this.count)
      .queries(`
      CREATE OR REPLACE TABLE ${dataform.projectConfig.vars.target_schema}.${dataform.projectConfig.vars.target_table}_final_table
       PARTITION BY ${dataform.projectConfig.vars.timestampfield}
      as (SELECT *, cast(null as DATE) as min_eff_date_source from ${dataform.projectConfig.vars.target_schema}.${dataform.projectConfig.vars.target_table}_child_delete_${this.count});
     
     
  `);
  
   }
    else {
// If the counter > 0, apply the Update -> Merge -> Insert Logic

  operate("scd2_merge_"+this.count)
  .dependencies("scd2_merge_" + (this.count-1))
  .dependencies("scd2_" + this.count)
  .queries(`
  -- target_columns returns a comma separated string of the target table from the metadata table 'INFORMATION_SCHEMA.COLUMNS'
  DECLARE target_columns STRING;
  
  SET target_columns = (
  WITH selected_columns as (
  SELECT column_name 
      FROM latika-experiments.${dataform.projectConfig.vars.target_schema}.INFORMATION_SCHEMA.COLUMNS
      WHERE 
      table_name =  "${dataform.projectConfig.vars.target_table}_final_table" and column_name not in (SELECT primary_key_column_name  
      FROM dataform_staging.table_to_unique
      WHERE 
      table_name =  "${dataform.projectConfig.vars.target_table}_final_table"  and table_catalog = "latika-experiments" and table_schema = "${dataform.projectConfig.vars.target_schema}"
  )  and column_name <> "min_eff_date_source" 
  )
  SELECT STRING_AGG(column_name) AS columns FROM selected_columns
  );
  
  -- ----------------------------------------------------------------------------------------------------------------------------
  -- Update min_eff_date_source from the target table is done to target the When Not Matched scenarios of the merge statement
  -- The purpose of it is to do the union of the last record of the target table (when exp_date is null) and the first two records
  -- of the child table (currently processing) for each partion where min(eff_date) of a unique key is same as  min(eff_date) of the child table.
  -- After union, it EXCEPTS the first partition that matches with the target table and assigns the min(eff_date) of the left over data. 
  -- This data is then joined with the distinct unique key, and max(date) of the target table.
  -- Post that using the coalesce function the right min_eff_date_source is updated on the target table
  -- ----------------------------------------------------------------------------------------------------------------------------

  
UPDATE ${dataform.projectConfig.vars.target_schema}.${dataform.projectConfig.vars.target_table}_final_table s
SET s.min_eff_date_source = DATE_ADD(out.min_eff_date, INTERVAL -1 DAY)
FROM (
select t1.${dataform.projectConfig.vars.target_hash_unique_col_name},
coalesce(t2.min_eff_date, DATE_ADD(cast(t1.max_eff_date as Date), INTERVAL 1 DAY)) as min_eff_date
from
(select max(${dataform.projectConfig.vars.timestampfield}) as max_eff_date, ${dataform.projectConfig.vars.target_hash_unique_col_name}, ${dataform.projectConfig.vars.end_at_column_name} from 
${dataform.projectConfig.vars.target_schema}.${dataform.projectConfig.vars.target_table}_final_table
where ${dataform.projectConfig.vars.end_at_column_name} is null group by ${dataform.projectConfig.vars.target_hash_unique_col_name}, ${dataform.projectConfig.vars.end_at_column_name}) t1 LEFT JOIN
( select min(a.min_eff_date) as min_eff_date, ${dataform.projectConfig.vars.target_hash_unique_col_name} from (
select
min(${dataform.projectConfig.vars.start_from_column_name}) as min_eff_date, ${dataform.projectConfig.vars.target_hash_unique_col_name} from (select * EXCEPT (min_eff_date_source) from ${dataform.projectConfig.vars.target_schema}.${dataform.projectConfig.vars.target_table}_final_table where ${dataform.projectConfig.vars.end_at_column_name} is null
union ALL
select * EXCEPT (r) from (select *, row_number() over (partition by ${dataform.projectConfig.vars.target_hash_unique_col_name} order by ${dataform.projectConfig.vars.start_from_column_name}) as r from ${dataform.projectConfig.vars.target_schema}.${dataform.projectConfig.vars.target_table}_child_delete_${this.count} where ${dataform.projectConfig.vars.target_hash_unique_col_name} IN (select ${dataform.projectConfig.vars.target_hash_unique_col_name} from ${dataform.projectConfig.vars.target_schema}.${dataform.projectConfig.vars.target_table}_child_delete_${this.count} where ${dataform.projectConfig.vars.start_from_column_name} =(select min(${dataform.projectConfig.vars.start_from_column_name}) from ${dataform.projectConfig.vars.target_schema}.${dataform.projectConfig.vars.target_table}_child_delete_${this.count} ))) where r < 3 ) group by ${dataform.projectConfig.vars.target_hash_unique_col_name}, ${dataform.projectConfig.vars.target_hash_non_unique_col_name}
except DISTINCT
select ${dataform.projectConfig.vars.start_from_column_name}, ${dataform.projectConfig.vars.target_hash_unique_col_name} from ${dataform.projectConfig.vars.target_schema}.${dataform.projectConfig.vars.target_table}_final_table where ${dataform.projectConfig.vars.end_at_column_name} is null
) a
group by ${dataform.projectConfig.vars.target_hash_unique_col_name}
) t2 ON t1.${dataform.projectConfig.vars.target_hash_unique_col_name} = t2.${dataform.projectConfig.vars.target_hash_unique_col_name}
where t1.${dataform.projectConfig.vars.end_at_column_name} is null
) out
WHERE s.${dataform.projectConfig.vars.target_hash_unique_col_name}=out.${dataform.projectConfig.vars.target_hash_unique_col_name} and ${dataform.projectConfig.vars.end_at_column_name} is null;
 
  
-- ----------------------------------------------------------------------------------------------------------------------------
-- The Merge Statement is used to smoothly bind all the child tables together considering a variety of data scenarios that can occur at 
-- the source side. 
-- WHEN MATCHED Conditions -> The two scenario that could happen is when we get the data from staging from the min(eff_date) of the child table and other being when we don't
-- WHEN MATCHED and eff_date = min(eff_date) of the child table Then Target Expiry DATE = Source Expiry DATE
-- WHEN MATCHED and readded_data_flag = Y Then Target Expiry DATE = Max {timestamp_field} of the Target Table 
-- Because we cannot insert data in the WHEN MATCHED of a merge statement the Insert Statement below addresses this scenario


-- WHEN MATCHED Conditions -> The two scenario that could happen is when we get the data from staging from the min(eff_date) of the child table and other being when we don't
-- For both the scenario, the min_eff_date_source will do the work to get the right expiry date for the Target.
-- But the inserts are only made on the flags where readded_data_flag = N to avoid duplicate inserting of data

-- ----------------------------------------------------------------------------------------------------------------------------

 
  EXECUTE IMMEDIATE format("""
  MERGE  ${dataform.projectConfig.vars.target_schema}.${dataform.projectConfig.vars.target_table}_final_table  as T
  USING (SELECT %s
  FROM ${dataform.projectConfig.vars.target_schema}.${dataform.projectConfig.vars.target_table}_child_delete_${this.count}
  ) AS s
  ON   T.${dataform.projectConfig.vars.target_hash_unique_col_name}  =  s.${dataform.projectConfig.vars.target_hash_unique_col_name}
          AND T.${dataform.projectConfig.vars.target_hash_non_unique_col_name} = s.${dataform.projectConfig.vars.target_hash_non_unique_col_name}
  AND t.${dataform.projectConfig.vars.end_at_column_name} is null 
   
  WHEN MATCHED AND t.${dataform.projectConfig.vars.end_at_column_name} is null and s.${dataform.projectConfig.vars.start_from_column_name} = (select min(${dataform.projectConfig.vars.start_from_column_name}) from ${dataform.projectConfig.vars.target_schema}.${dataform.projectConfig.vars.target_table}_child_delete_${this.count} )THEN
  UPDATE SET T.${dataform.projectConfig.vars.end_at_column_name} = s.${dataform.projectConfig.vars.end_at_column_name}

 WHEN  MATCHED AND t.${dataform.projectConfig.vars.end_at_column_name}  is null and s.readded_data_flag = 'Y' THEN
  UPDATE SET ${dataform.projectConfig.vars.end_at_column_name}  = (select max(${dataform.projectConfig.vars.timestampfield}) from ${dataform.projectConfig.vars.target_schema}.${dataform.projectConfig.vars.target_table}_final_table
)

  WHEN NOT MATCHED BY SOURCE AND t.${dataform.projectConfig.vars.end_at_column_name} is null THEN
  UPDATE SET ${dataform.projectConfig.vars.end_at_column_name} = T.min_eff_date_source
  WHEN NOT MATCHED BY TARGET and s.readded_data_flag = 'N' THEN
    INSERT(  %s, min_eff_date_source)
    VALUES(%s, null)
  """,target_columns, target_columns, target_columns);
  
   
  EXECUTE IMMEDIATE format("""
INSERT INTO dataform_scd_backfill.covid_final_final_table (%s, min_eff_date_source) (
select %s ,null as min_eff_date_source  from ${dataform.projectConfig.vars.target_schema}.${dataform.projectConfig.vars.target_table}_child_delete_${this.count} where readded_data_flag = 'Y' 

)
  """, target_columns, target_columns);

  `).tags(["scd2_backfill"])
    }
  }

// ---------------------------------------------------------------------------------------------------
// End Creating and processing of Final Table
// ---------------------------------------------------------------------------------------------------

  