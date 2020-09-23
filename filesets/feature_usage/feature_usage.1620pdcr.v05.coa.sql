/*  extracts the feature logging from dbqlogtbl WITHOUT the cartesian Join
    by user bucket / department. Mapping to Feature_IDs happen in Transcend.

  parameters
     siteid     = {siteid}
     startdate  = {startdate}
     enddate    = {enddate}
*/

create volatile table Feature_Log as
(
    with dbql as (
      select
       dbq.LogDate
      ,dbq.featureusage
      ,usr.UserName
      ,usr.UserHash
      ,usr.User_Bucket
      ,usr.User_Department
      ,usr.User_SubDepartment
      ,usr.User_Region
      ,count(*) as rec_count
      from pdcrinfo.dbqlogtbl_hst as dbq
      join dim_user as usr
        on usr.UserName = dbq.UserName
      where dbq.LogDate between {startdate} and {enddate}
        and featureusage is not null
      group by
      dbq.LogDate
     ,dbq.featureusage
     ,usr.UserName
     ,usr.UserHash
     ,usr.User_Bucket
     ,usr.User_Department
     ,usr.User_SubDepartment
     ,usr.User_Region
    )
    select
     dbql.LogDate
    ,feat.featurename
    ,feat.featurebitpos as BitPos
    ,dbql.UserName
    ,dbql.UserHash
    ,dbql.User_Bucket
    ,dbql.User_Department
    ,dbql.User_SubDepartment
    ,dbql.User_Region
    ,sum(zeroifnull(dbql.rec_count)) as Query_Cnt
    from dbc.qrylogfeaturelistv feat
    left join dbql
      on bytes(dbql.featureusage) = 256
     and getbit(dbql.featureusage,(2047-feat.featurebitpos)) = 1
    group by
    dbql.LogDate
   ,feat.featurename
   ,feat.featurebitpos
   ,dbql.UserName
   ,dbql.UserHash
   ,dbql.User_Bucket
   ,dbql.User_Department
   ,dbql.User_SubDepartment
   ,dbql.User_Region
) with data
  No Primary Index
  on commit preserve rows
;


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
from Feature_Log
group by 1,2,3,4,5,6,7,8
order by 9 desc
;
