/* BizUsageDecoded Lite 2.5 -- Query 4

Parameters:
  siteid    = {siteid}
  startdate = {startdate}
  enddaate  = {enddate}
  dbqlogtbl = {dbqlogtbl}

 */

/*{{save:bizusedecodelite_Q4_{siteid}.csv}}*/
LOCKING ROW FOR ACCESS
SELECT
    '{siteid}' as "Site Id"
    ,LogDate as "Log Date"
    , LogIORat as "Logical IO Ratio"
    , PossExCPU/QueryCPU  as "Possible Pct of CPU Used for Extracts"
FROM (
select
  logdate
  ,sum(case when UII > 1e1 then TotalIOCount else 0e0 end) PossLogIO
  , sum(TotalIOCOunt) QueryLogIO -- intermediate
  , PossLogIO / QueryLogIO as LogIORat  -- "Extract Index"
  , sum(case when UII > 1e1 then AMPCPUTime else 0e0 end) as PossExCPU
  , sum(AMPCPUTime) as QueryCPU
from (
  Sel logdate, AMPCPUTime ,TotalIOCount, ReqPhysIO, NumResultRows
    ,  Case when AMPCPUTime < 1e-1  or StatementType in all('Insert','Update','Delete','End loading','create table','checkpoint loading','help','collect statistics')
                          or NumSteps = 0 or NumOfActiveAMPS = 0 then 0e0
            else TotalIOCount/(AMPCPUTime *1e3) end  as UII
  From {dbqlogtbl} /*pdcrinfo.DBQLogTbl_hst */
where  logdate between {startdate} and {enddate}
and
  NumSteps > 0 and AMPCPUTime > 0e0 and NumOfActiveAMPS > 0
  and AppID <> 'DSMAIN' and UserName <> 'MOSDECODE02') x1
  Group by 1

  ) extracts
