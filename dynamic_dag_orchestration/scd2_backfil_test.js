constructor() 
{
    this.count = 0;
}

var daylist_str = dataform.projectConfig.vars.daylist;
daylist_str = daylist_str.replace(/'/g, '"'); //replacing all ' with "
var daylist = JSON.parse(daylist_str);

daylist.map(pair => pair.split(":")).forEach(daylist => {
console.log("Daylist - " +daylist);
operate("scd2_child_" + this.count,{
    hasOutput: true
  }).queries(`
  
  CREATE OR REPLACE TABLE dataform.scd2_test_child_${this.count} AS
  SELECT 
      cast('${daylist[0]}' as Date) as start_date,
      cast('${daylist[1]}' as Date) as end_date`
  ).tags("[scd2_backfill_test]"),
  
  ++this.count;

}
);

var max_count = this.count;
for(this.count = 0; this.count< max_count; ++this.count){
// If the counter is 0, create a new table and insert the data of Child Table 0
if (this.count ==0)
{
  operate("scd2_merge_" + this.count)
  .dependencies("scd2_child_" + this.count)
  .queries(`
  CREATE OR REPLACE TABLE dataform.scd2_backfill_final_table
  as (SELECT * from dataform.scd2_child_${this.count});
  `).tags("[scd2_backfill_test]");
}
// If the counter > 0, insert rows from the rest of the child tables
else 
{
  operate("scd2_merge_" + this.count)
  .dependencies("scd2_child_" + this.count)
  .queries(`
    INSERT INTO dataform.scd2_backfill_final_table (
      start_date,
      end_date
    )
    SELECT start_date, end_date FROM dataform.scd2_child_${this.count};
  `).tags("[scd2_backfill_test]");
}
}