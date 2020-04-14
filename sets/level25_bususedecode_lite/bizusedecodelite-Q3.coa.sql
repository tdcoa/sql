/* BizUsageDecoded Lite 2.5 -- Query 3

Parameters:
  siteid    = {siteid}
  startdate = {startdate}
  enddaate  = {enddate}
  dbqlogtbl = {dbqlogtbl}

 */


/*{{save:bizusedecodelite_Q3_{siteid}.csv}}*/
lock row access
select
    '{siteid}' as "Site Id"
    ,'Detail' (CHAR(7)) as "Log Type"
    ,Logdate as "Log Date"
    ,appid as "App Id"
    ,case when GetQuerybandvalue(Queryband, 0,'Clientuser') like any ('0%','1%','2%','3%','4%','5%','6%','7%','8%','9%')
          then GetQuerybandvalue(Queryband, 0,'Clientuser') else username end as Username
    ,case when errorcode = 3156 then 'Aborts' else 'Normal' end as "Abort Indicator"
    ,case when StatementType in ('Select','Collect Statistics' ) then StatementType else 'Ingest/ETL' end as "Statement Group"
    ,SUM(cast(ampcputime as FLOAT)) "Total AMP CPU Seconds"
    ,cast(count(*) as FLOAT) "Query Count"
    ,SUM(cast(SpoolUsage as FLOAT))/1073741824.000000 "Total Spool Usage GB"
    ,SUM(cast(TotalIOCount as FLOAT)) "Total IO Count"
    ,sum(cast(ReqIOKB as FLOAT))/1048576.000000  "Total Req IO GB"
    ,sum(cast(ReqPhysIO as FLOAT))        as "Total Req Phys IO"
    ,sum(cast(ReqPhysIOKB as FLOAT))/1048576.000000 "Total Req Phys IO GB"
from {dbqlogtbl} /* pdcrinfo.dbqlogtbl_hst */
where   logdate between {startdate} and {enddate}
group by 1,2,3,4,5,6,7
;
