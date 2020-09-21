/*  extracts the feature logging from dbqlogtbl WITHOUT the cartesian Join
    by user bucket / department. Mapping to Feature_IDs happen in Transcend.

  parameters
     dbqlogtbl  = {dbqlogtbl}
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
      ,count(*) as Query_Cnt
      from pdcrinfo.dbqlogtbl_hst a
      where logdate between {startdate} and {enddate}
	  and featureusage is not null
      group by 1,2
    )
    select
     dbql.LogDate
    ,feat.featurename
    ,sum(zeroifnull(dbql.Query_Cnt)) Tot_cnt
    from dbc.qrylogfeaturelistv feat 
    left join dbql
      on bytes(dbql.featureusage) = 256
     and getbit(dbql.featureusage,(2047-feat.featurebitpos)) = 1
    group by LogDate, FeatureName
) with data
  No Primary Index
  on commit preserve rows
;



/*{{save:feature_usage.csv}}*/
/*{{load:{db_stg}.stg_dat_feature_usage_log}}*/
/*{{call:{db_coa}.sp_dat_feature_usage_log('v1')}}*/
select
 x.LogDate
,x.FeatureName
,x.BitPos
,coalesce(l.Request_Cnt,0) as Request_Cnt
from Feature_Log as l
right outer join
    (Select Calendar_Date as LogDate, FeatureBitPos as BitPos, FeatureName
     from sys_calendar.calendar cross join dbc.qrylogfeaturelistv
     where Calendar_Date between DATE-2 and DATE-1) as x
 on l.LogDate = x.LogDate
and l.BitPos = x.BitPos
;
