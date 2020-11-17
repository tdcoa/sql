/*  Selects the content of the Volatile table created in the prev step

  parameters
     siteid     = {siteid}
*/

/*{{save:feature_usage.csv}}*/
/*{{load:{db_stg}.stg_dat_feature_usage_log}}*/
/*{{call:{db_coa}.sp_dat_feature_usage_log('v1')}}*/
select
 '{siteid}' as Site_ID
,cast(LogDate as format 'Y4-MM-DD') as LogDate
,featurename as Feature_Name
,BitPos
,User_Bucket
,User_Department
,User_SubDepartment
,User_Region
,sum(Query_Cnt) as Feature_Usage_Cnt
from Feature_Usage
group by 1,2,3,4,5,6,7,8
order by 9 desc
;
