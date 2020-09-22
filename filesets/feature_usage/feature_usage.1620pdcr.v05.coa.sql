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
       LogDate
      ,featureusage
      ,count(*) as rec_count
      from pdcrinfo.dbqlogtbl_hst a
      where logdate between {startdate} and {enddate}
        and featureusage is not null
      group by 1,2
    )
    select
     dbql.LogDate
    ,feat.featurename
    ,feat.featurebitpos as BitPos
    ,sum(zeroifnull(dbql.rec_count)) as Query_Cnt
    from dbc.qrylogfeaturelistv feat
    left join dbql
      on bytes(dbql.featureusage) = 256
     and getbit(dbql.featureusage,(2047-feat.featurebitpos)) = 1
    group by LogDate, FeatureName, BitPos
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
,featurename
,BitPos
,Query_Cnt
from Feature_Log
order by Query_Cnt desc
;
