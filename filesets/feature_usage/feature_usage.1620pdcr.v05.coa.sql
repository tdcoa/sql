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
      ,count(*) as Request_Cnt
      from pdcrinfo.dbqlogtbl_hst a
      where logdate between DATE-9 and DATE-1
        and featureusage is not null
      group by 1,2
    )
    select
     dbql.LogDate
    ,feat.FeatureName
    ,feat.featurebitpos as BitPos
    ,sum(dbql.Request_Cnt) as Request_Cnt
    from dbql
    inner join dbc.qrylogfeaturelistv feat
      on bytes(dbql.featureusage) = 256
     and getbit(dbql.featureusage,(2047-feat.featurebitpos)) = 1
    group by LogDate, FeatureName, featurebitpos
) with data
  No Primary Index
  on commit preserve rows
;



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
