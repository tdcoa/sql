/* Start COA: DBQL_Core
   see comments about each SQL step inline below.

Parameters:
  - startdate:    {startdate}
  - enddate:      {enddate}
  - siteid:       {siteid}
  - dbqlogtbl:    {dbqlogtbl}
  - resusagespma: {resusagespma}
*/

/*{{save:UTC_Offset.csv}}*/
SELECT SUBSTRING ((Current_Time (FORMAT 'HH:MI:SS.S(F)Z') (VARCHAR (20))) FROM 9 FOR 6) as UTC_Offset
;


create volatile table cpu_summary_hourly as
(
Select
 cast(TheDate as format 'Y4-MM-DD') as LogDate
,Floor(TheTime/1e4) as LogHour
,SUBSTRING ((Current_Time (FORMAT 'HH:MI:SS.S(F)Z') (VARCHAR (20))) FROM 9 FOR 6) as UTC_Offset
,cast(max(NodeType) as varchar(10)) as Node_Type
,cast(count(distinct NodeID) as smallint) as Node_Cnt
,cast(max(NCPUs) as smallint) as vCPU_per_Node
,sum(cast(FullPotentialIOTA/1e9 as decimal(18,0))) as MaxIOTA_cntB
,sum(cast(CPUIdle   as decimal(18,2))) as CPU_Idle
,sum(cast(CPUIOWait as decimal(18,2))) as CPU_IOWait
,sum(cast(CPUUServ  as decimal(18,2))) as CPU_OS
,sum(cast(CPUUExec  as decimal(18,2))) as CPU_DBS
,CPU_Idle+CPU_IOWait+CPU_OS+CPU_DBS as CPU_Total
,SUM(HostReadKB) as HostRead_KB
,SUM(HostWriteKB) as HostWrite_KB
from pdcrinfo.ResUsageSPMA_Hst
where TheDate between {startdate} and {enddate}
Group by LogDate, LogHour
) with data
no primary index on commit preserve rows;


/* ResUsage_MaxCPU
   This information is saved and loaded into Transcend, then
   joined back to the DBQL_Core process in the view tier
*/



SET TIME ZONE 'GMT'
/* allows for local-time adjusted 'business hours' analysis  */
;

create volatile table dbql_core_hourly as
(
SELECT
   cast(cast(starttime as format 'YYYY-MM-DDBHH') AS CHAR(13)) || ':00:00' as LogTS
  ,cast(HashAmp()+1 as Integer) as Total_AMPs
  ,username
  ,appid
  ,StatementType

  /* ====== Query Metrics ======= */
  ,zeroifnull(cast(count(1) as BigInt)) as Request_Cnt
  ,zeroifnull(sum(cast( dbql.Statements as BigInt))) as Query_Cnt
  ,zeroifnull(sum(cast(case when dbql.StatementGroup like '%=%' then 1 else 0 end as SmallInt))) as Query_MultiStatement_Cnt
  /* ErrorCode 3158 == TASM Demotion, aka warning, not real error */
  ,zeroifnull(sum(cast(case when dbql.ErrorCode not in(0,3158)      then dbql.Statements else 0 end as int))) as Query_Error_Cnt
  ,zeroifnull(sum(cast(case when dbql.Abortflag = 'T'               then dbql.Statements else 0 end as int))) as Query_Abort_Cnt
  ,zeroifnull(sum(cast(case when TotalIOCount = 0                   then dbql.Statements else 0 end as int))) as Query_NoIO_cnt
  ,zeroifnull(sum(cast(case when TotalIOCount > 0 AND ReqPhysIO = 0 then dbql.Statements else 0 end as int))) as Query_InMem_Cnt
  ,zeroifnull(sum(cast(case when TotalIOCount > 0 AND ReqPhysIO > 0 then dbql.Statements else 0 end as int))) as Query_PhysIO_Cnt
  ,zeroifnull(sum(cast(case
           when dbql.StatementType = 'Select'
            and dbql.NumOfActiveAMPs < (Total_AMPs * 0.10)
            and (ZEROIFNULL( CAST(
               (EXTRACT(HOUR   FROM ((FirstRespTime - FirstStepTime) HOUR(3) TO SECOND(6)) ) * 3600)
              +(EXTRACT(MINUTE FROM ((FirstRespTime - FirstStepTime) HOUR(3) TO SECOND(6)) ) *   60)
              +(EXTRACT(SECOND FROM ((FirstRespTime - FirstStepTime) HOUR(3) TO SECOND(6)) ) *    1)
               as FLOAT))) <= 1  /* Runtime_AMP_Sec */
           then 1 else 0 end as Integer))) as Query_Tactical_Cnt
  ,cast(avg(dbql.NumSteps) as decimal(15,4)) as Query_Complexity_Score_Avg
  ,zeroifnull(sum(cast(dbql.NumResultRows as BigInt) )) as Returned_Row_Cnt

  /* ====== Metrics: RunTimes ====== */
  ,sum(cast(zeroifnull(dbql.DelayTime) as decimal(18,2))) as DelayTime_Sec
  ,sum(ZEROIFNULL(CAST(
     (EXTRACT(HOUR   FROM ((FirstStepTime - StartTime) HOUR(3) TO SECOND(6)) ) * 3600)
    +(EXTRACT(MINUTE FROM ((FirstStepTime - StartTime) HOUR(3) TO SECOND(6)) ) *   60)
    +(EXTRACT(SECOND FROM ((FirstStepTime - StartTime) HOUR(3) TO SECOND(6)) ) *    1)
     as FLOAT))) as RunTime_Parse_Sec
  ,sum(ZEROIFNULL(CAST(
     (EXTRACT(HOUR   FROM ((FirstRespTime - FirstStepTime) HOUR(3) TO SECOND(6)) ) * 3600)
    +(EXTRACT(MINUTE FROM ((FirstRespTime - FirstStepTime) HOUR(3) TO SECOND(6)) ) *   60)
    +(EXTRACT(SECOND FROM ((FirstRespTime - FirstStepTime) HOUR(3) TO SECOND(6)) ) *    1)
     as FLOAT))) as Runtime_AMP_Sec
  ,sum(TotalFirstRespTime)  as RunTime_Total_Sec
  ,sum(ZEROIFNULL(CAST(
     case when LastRespTime is not null then
     (EXTRACT(HOUR   FROM ((LastRespTime - FirstRespTime) HOUR(3) TO SECOND(6)) ) * 3600)
    +(EXTRACT(MINUTE FROM ((LastRespTime - FirstRespTime) HOUR(3) TO SECOND(6)) ) *   60)
    +(EXTRACT(SECOND FROM ((LastRespTime - FirstRespTime) HOUR(3) TO SECOND(6)) ) *    1)
    else 0 end as FLOAT))) AS TransferTime_Sec

  /* ====== Metrics: CPU & IO ====== */
  ,zeroifnull(sum( cast(dbql.ParserCPUTime    as decimal(18,2)))) as CPU_Parse_Sec
  ,zeroifnull(sum( cast(dbql.AMPCPUtime       as decimal(18,2)))) as CPU_AMP_Sec
  /* TODO: check if failed queries log CPU consumption */
  ,zeroifnull(sum( cast(ReqPhysIO/1e6         as decimal(18,2)))) as IOCntM_Physical
  ,zeroifnull(sum( cast(TotalIOCount/1e6      as decimal(18,2)))) as IOCntM_Total
  ,zeroifnull(sum( cast(ReqPhysIOKB/1e6       as decimal(18,2)))) as IOGB_Physical
  ,zeroifnull(sum( cast(ReqIOKB/1e6           as decimal(18,2)))) as IOGB_Total
  ,zeroifnull(sum( cast(dbql.UsedIOTA/1e9     as decimal(18,2)))) as IOTA_Used_cntB

  /* ====== Metrics: Other ====== */
  ,zeroifnull(cast(avg(NumOfActiveAMPs) as decimal(18,4))) as NumOfActiveAMPs_Avg
  ,zeroifnull(sum(SpoolUsage/1e9))  as Spool_GB
  ,zeroifnull(avg( cast(1-(ReqPhysIO/nullifzero(TotalIOCount)) as decimal(32,6)) )) as CacheHit_Pct
  ,zeroifnull(avg((AMPCPUTime / nullifzero(MaxAmpCPUTime*NumOfActiveAMPs))-1)) as CPUSec_Skew_AvgPCt
  ,zeroifnull(avg((TotalIOCount / nullifzero(MaxAmpIO*NumOfActiveAMPs))-1) )   as IOCnt_Skew_AvgPct

  From pdcrinfo.DBQLogTbl_Hst as dbql
  where dbql.LogDate between {startdate} and {enddate}
  Group by
   LogTS
  ,username
  ,appid
  ,StatementType

union all

SELECT
   cast(cast(starttime as format 'YYYY-MM-DDBHH') AS CHAR(13)) || ':00:00' as LogTS
  ,HashAmp() + 1 as Total_AMPs
  ,username
  ,appid
  ,'Summary' as StatementType
  ,zeroifnull(sum(cast(smry.QueryCount as decimal(18,2)))) as Request_Cnt
  ,zeroifnull(sum(cast(smry.QueryCount as decimal(18,2)))) as Query_Cnt
  ,null as Query_MultiStatement_Cnt
  ,null as Query_Error_Cnt
  ,null as Query_Abort_Cnt
  ,null as Query_NoIO_cnt
  ,null as Query_InMem_Cnt
  ,null as Query_PhysIO_Cnt
  ,null as Query_Tactical_Cnt
  ,cast(null as float) as Query_Complexity_Score_Avg
  ,null as Returned_Row_Cnt
  ,null as DelayTime_Sec
  ,null as RunTime_Parse_Sec
  ,null as Runtime_AMP_Sec
  ,zeroifnull(sum(cast(smry.QuerySeconds as decimal(18,2)))) as RunTime_Total_Sec
  ,null as TransferTime_Sec
  ,zeroifnull(sum(cast(smry.ParserCPUTime as decimal(18,2)))) as CPU_Parse_Sec
  ,zeroifnull(sum(cast(smry.AMPCPUTime as decimal(18,2)))) as CPU_AMP_Sec
  ,zeroifnull(sum(cast(smry.ReqPhysIO/1e6 as decimal(18,2)))) as IOCntM_Physical
  ,zeroifnull(sum(cast(smry.TotalIOCount/1e6 as decimal(18,2)))) as IOCntM_Total
  ,zeroifnull(sum(cast(smry.ReqPhysIOKB/1e6 as decimal(18,2)))) as IOGB_Physical
  ,null as IOGB_Total
  ,zeroifnull(sum(cast(smry.UsedIota/1e9 as decimal(18,2)))) as IOTA_Used_cntB
  ,cast(null as float) as NumOfActiveAMPs_Avg
  ,null as Spool_GB
  ,zeroifnull(avg( cast(1-(ReqPhysIO/nullifzero(TotalIOCount)) as decimal(32,6)) )) as CacheHit_Pct
  ,null as CPUSec_Skew_AvgPCt
  ,null as IOCnt_Skew_AvgPct

  From PDCRINFO.DBQLSummaryTbl_Hst smry
  where smry.LogDate between {startdate} and {enddate}
  Group by
   LogTS
  ,username
  ,appid
  ,StatementType
) with data
no primary index on commit preserve rows
;


/*
 DBQL_CORE ()
=========================
Aggregates DBQL into a per-day, per-hour, per-DIM (app/statement/user) buckets
as defined above.  The intention is this to be a smaller table than the
detail, however, this assumption largely relies on how well the bucketing
logic is defined / setup above.  If the result set is too large, try
revisiting the Bucket definitions above, and make groups smaller / less varied.

Also - many compound metrics have been stripped, to minimize transfer file
size.  As these fields are easily calculated, they will be re-constituted
in Transcend.
*/


/* Query_Breakouts by User Buckets */

create volatile table dbql_core_breakout as
(
  SELECT /*dbql_core*/
   '{siteid}'  as Site_ID
  ,cast(LogDate as format 'Y4-MM-DD') as LogDate
  ,usr.User_Bucket
  ,usr.User_Department
  ,usr.User_SubDepartment

  /* Query Runtime by [query count | cpu | iogb] */
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.TotalFirstRespTime  is NULL OR  dbql.TotalFirstRespTime <1     THEN dbql.Statements ELSE 0 END AS INTEGER)))   as qrycnt_in_runtime_0000_0001
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.TotalFirstRespTime  >=1    AND  dbql.TotalFirstRespTime <5     THEN dbql.Statements ELSE 0 END AS INTEGER)))   as qrycnt_in_runtime_0001_0005
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.TotalFirstRespTime  >=5    AND  dbql.TotalFirstRespTime <10    THEN dbql.Statements ELSE 0 END AS INTEGER)))   as qrycnt_in_runtime_0005_0010
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.TotalFirstRespTime  >=10   AND  dbql.TotalFirstRespTime <30    THEN dbql.Statements ELSE 0 END AS INTEGER)))   as qrycnt_in_runtime_0010_0030
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.TotalFirstRespTime  >=30   AND  dbql.TotalFirstRespTime <60    THEN dbql.Statements ELSE 0 END AS INTEGER)))   as qrycnt_in_runtime_0030_0060
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.TotalFirstRespTime  >=60   AND  dbql.TotalFirstRespTime <300   THEN dbql.Statements ELSE 0 END AS INTEGER)))   as qrycnt_in_runtime_0060_0300
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.TotalFirstRespTime  >=300  AND  dbql.TotalFirstRespTime <600   THEN dbql.Statements ELSE 0 END AS INTEGER)))   as qrycnt_in_runtime_0300_0600
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.TotalFirstRespTime  >=600  AND  dbql.TotalFirstRespTime <1800  THEN dbql.Statements ELSE 0 END AS INTEGER)))   as qrycnt_in_runtime_0600_1800
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.TotalFirstRespTime  >=1800 AND  dbql.TotalFirstRespTime <3600  THEN dbql.Statements ELSE 0 END AS INTEGER)))   as qrycnt_in_runtime_1800_3600
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.TotalFirstRespTime  >3600                                      THEN dbql.Statements ELSE 0 END AS INTEGER)))   as qrycnt_in_runtime_3600_plus

  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.TotalFirstRespTime  is NULL OR  dbql.TotalFirstRespTime <1     THEN dbql.AMPCPUtime + dbql.ParserCPUTime ELSE 0 END AS INTEGER)))  as cpusec_in_runtime_0000_0001
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.TotalFirstRespTime  >=1    AND  dbql.TotalFirstRespTime <5     THEN dbql.AMPCPUtime + dbql.ParserCPUTime ELSE 0 END AS INTEGER)))  as cpusec_in_runtime_0001_0005
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.TotalFirstRespTime  >=5    AND  dbql.TotalFirstRespTime <10    THEN dbql.AMPCPUtime + dbql.ParserCPUTime ELSE 0 END AS INTEGER)))  as cpusec_in_runtime_0005_0010
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.TotalFirstRespTime  >=10   AND  dbql.TotalFirstRespTime <30    THEN dbql.AMPCPUtime + dbql.ParserCPUTime ELSE 0 END AS INTEGER)))  as cpusec_in_runtime_0010_0030
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.TotalFirstRespTime  >=30   AND  dbql.TotalFirstRespTime <60    THEN dbql.AMPCPUtime + dbql.ParserCPUTime ELSE 0 END AS INTEGER)))  as cpusec_in_runtime_0030_0060
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.TotalFirstRespTime  >=60   AND  dbql.TotalFirstRespTime <300   THEN dbql.AMPCPUtime + dbql.ParserCPUTime ELSE 0 END AS INTEGER)))  as cpusec_in_runtime_0060_0300
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.TotalFirstRespTime  >=300  AND  dbql.TotalFirstRespTime <600   THEN dbql.AMPCPUtime + dbql.ParserCPUTime ELSE 0 END AS INTEGER)))  as cpusec_in_runtime_0300_0600
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.TotalFirstRespTime  >=600  AND  dbql.TotalFirstRespTime <1800  THEN dbql.AMPCPUtime + dbql.ParserCPUTime ELSE 0 END AS INTEGER)))  as cpusec_in_runtime_0600_1800
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.TotalFirstRespTime  >=1800 AND  dbql.TotalFirstRespTime <3600  THEN dbql.AMPCPUtime + dbql.ParserCPUTime ELSE 0 END AS INTEGER)))  as cpusec_in_runtime_1800_3600
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.TotalFirstRespTime  >3600                                      THEN dbql.AMPCPUtime + dbql.ParserCPUTime ELSE 0 END AS INTEGER)))  as cpusec_in_runtime_3600_plus

  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.TotalFirstRespTime  is NULL OR  dbql.TotalFirstRespTime <1     THEN ReqIOKB/1e6 ELSE 0 END AS INTEGER)))   as iogb_in_runtime_0000_0001
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.TotalFirstRespTime  >=1    AND  dbql.TotalFirstRespTime <5     THEN ReqIOKB/1e6 ELSE 0 END AS INTEGER)))   as iogb_in_runtime_0001_0005
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.TotalFirstRespTime  >=5    AND  dbql.TotalFirstRespTime <10    THEN ReqIOKB/1e6 ELSE 0 END AS INTEGER)))   as iogb_in_runtime_0005_0010
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.TotalFirstRespTime  >=10   AND  dbql.TotalFirstRespTime <30    THEN ReqIOKB/1e6 ELSE 0 END AS INTEGER)))   as iogb_in_runtime_0010_0030
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.TotalFirstRespTime  >=30   AND  dbql.TotalFirstRespTime <60    THEN ReqIOKB/1e6 ELSE 0 END AS INTEGER)))   as iogb_in_runtime_0030_0060
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.TotalFirstRespTime  >=60   AND  dbql.TotalFirstRespTime <300   THEN ReqIOKB/1e6 ELSE 0 END AS INTEGER)))   as iogb_in_runtime_0060_0300
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.TotalFirstRespTime  >=300  AND  dbql.TotalFirstRespTime <600   THEN ReqIOKB/1e6 ELSE 0 END AS INTEGER)))   as iogb_in_runtime_0300_0600
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.TotalFirstRespTime  >=600  AND  dbql.TotalFirstRespTime <1800  THEN ReqIOKB/1e6 ELSE 0 END AS INTEGER)))   as iogb_in_runtime_0600_1800
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.TotalFirstRespTime  >=1800 AND  dbql.TotalFirstRespTime <3600  THEN ReqIOKB/1e6 ELSE 0 END AS INTEGER)))   as iogb_in_runtime_1800_3600
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.TotalFirstRespTime  >3600                                      THEN ReqIOKB/1e6 ELSE 0 END AS INTEGER)))   as iogb_in_runtime_3600_plus


  /* delaytime by [query count | cpu | iogb] */
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.delaytime  is NULL OR  dbql.delaytime <1     THEN dbql.Statements ELSE 0 END AS INTEGER))) as qrycnt_in_delaytime_0000_0001
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.delaytime  >=1    AND  dbql.delaytime <5     THEN dbql.Statements ELSE 0 END AS INTEGER))) as qrycnt_in_delaytime_0001_0005
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.delaytime  >=5    AND  dbql.delaytime <10    THEN dbql.Statements ELSE 0 END AS INTEGER))) as qrycnt_in_delaytime_0005_0010
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.delaytime  >=10   AND  dbql.delaytime <30    THEN dbql.Statements ELSE 0 END AS INTEGER))) as qrycnt_in_delaytime_0010_0030
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.delaytime  >=30   AND  dbql.delaytime <60    THEN dbql.Statements ELSE 0 END AS INTEGER))) as qrycnt_in_delaytime_0030_0060
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.delaytime  >=60   AND  dbql.delaytime <300   THEN dbql.Statements ELSE 0 END AS INTEGER))) as qrycnt_in_delaytime_0060_0300
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.delaytime  >=300  AND  dbql.delaytime <600   THEN dbql.Statements ELSE 0 END AS INTEGER))) as qrycnt_in_delaytime_0300_0600
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.delaytime  >=600  AND  dbql.delaytime <1800  THEN dbql.Statements ELSE 0 END AS INTEGER))) as qrycnt_in_delaytime_0600_1800
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.delaytime  >=1800 AND  dbql.delaytime <3600  THEN dbql.Statements ELSE 0 END AS INTEGER))) as qrycnt_in_delaytime_1800_3600
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.delaytime  >3600                             THEN dbql.Statements ELSE 0 END AS INTEGER))) as qrycnt_in_delaytime_3600_plus

  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.delaytime  is NULL OR  dbql.delaytime <1     THEN dbql.AMPCPUtime + dbql.ParserCPUTime ELSE 0 END AS INTEGER))) as cpusec_in_delaytime_0000_0001
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.delaytime  >=1    AND  dbql.delaytime <5     THEN dbql.AMPCPUtime + dbql.ParserCPUTime ELSE 0 END AS INTEGER))) as cpusec_in_delaytime_0001_0005
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.delaytime  >=5    AND  dbql.delaytime <10    THEN dbql.AMPCPUtime + dbql.ParserCPUTime ELSE 0 END AS INTEGER))) as cpusec_in_delaytime_0005_0010
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.delaytime  >=10   AND  dbql.delaytime <30    THEN dbql.AMPCPUtime + dbql.ParserCPUTime ELSE 0 END AS INTEGER))) as cpusec_in_delaytime_0010_0030
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.delaytime  >=30   AND  dbql.delaytime <60    THEN dbql.AMPCPUtime + dbql.ParserCPUTime ELSE 0 END AS INTEGER))) as cpusec_in_delaytime_0030_0060
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.delaytime  >=60   AND  dbql.delaytime <300   THEN dbql.AMPCPUtime + dbql.ParserCPUTime ELSE 0 END AS INTEGER))) as cpusec_in_delaytime_0060_0300
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.delaytime  >=300  AND  dbql.delaytime <600   THEN dbql.AMPCPUtime + dbql.ParserCPUTime ELSE 0 END AS INTEGER))) as cpusec_in_delaytime_0300_0600
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.delaytime  >=600  AND  dbql.delaytime <1800  THEN dbql.AMPCPUtime + dbql.ParserCPUTime ELSE 0 END AS INTEGER))) as cpusec_in_delaytime_0600_1800
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.delaytime  >=1800 AND  dbql.delaytime <3600  THEN dbql.AMPCPUtime + dbql.ParserCPUTime ELSE 0 END AS INTEGER))) as cpusec_in_delaytime_1800_3600
  ,zeroifnull(SUM(CAST(CASE WHEN  dbql.delaytime  >3600                             THEN dbql.AMPCPUtime + dbql.ParserCPUTime ELSE 0 END AS INTEGER))) as cpusec_in_delaytime_3600_plus

  ,zeroifnull(SUM(CAST(CASE WHEN dbql.delaytime  is NULL OR  dbql.delaytime <1     THEN ReqIOKB/1e6 ELSE 0 END AS INTEGER))) as iogb_in_delaytime_0000_0001
  ,zeroifnull(SUM(CAST(CASE WHEN dbql.delaytime  >=1    AND  dbql.delaytime <5     THEN ReqIOKB/1e6 ELSE 0 END AS INTEGER))) as iogb_in_delaytime_0001_0005
  ,zeroifnull(SUM(CAST(CASE WHEN dbql.delaytime  >=5    AND  dbql.delaytime <10    THEN ReqIOKB/1e6 ELSE 0 END AS INTEGER))) as iogb_in_delaytime_0005_0010
  ,zeroifnull(SUM(CAST(CASE WHEN dbql.delaytime  >=10   AND  dbql.delaytime <30    THEN ReqIOKB/1e6 ELSE 0 END AS INTEGER))) as iogb_in_delaytime_0010_0030
  ,zeroifnull(SUM(CAST(CASE WHEN dbql.delaytime  >=30   AND  dbql.delaytime <60    THEN ReqIOKB/1e6 ELSE 0 END AS INTEGER))) as iogb_in_delaytime_0030_0060
  ,zeroifnull(SUM(CAST(CASE WHEN dbql.delaytime  >=60   AND  dbql.delaytime <300   THEN ReqIOKB/1e6 ELSE 0 END AS INTEGER))) as iogb_in_delaytime_0060_0300
  ,zeroifnull(SUM(CAST(CASE WHEN dbql.delaytime  >=300  AND  dbql.delaytime <600   THEN ReqIOKB/1e6 ELSE 0 END AS INTEGER))) as iogb_in_delaytime_0300_0600
  ,zeroifnull(SUM(CAST(CASE WHEN dbql.delaytime  >=600  AND  dbql.delaytime <1800  THEN ReqIOKB/1e6 ELSE 0 END AS INTEGER))) as iogb_in_delaytime_0600_1800
  ,zeroifnull(SUM(CAST(CASE WHEN dbql.delaytime  >=1800 AND  dbql.delaytime <3600  THEN ReqIOKB/1e6 ELSE 0 END AS INTEGER))) as iogb_in_delaytime_1800_3600
  ,zeroifnull(SUM(CAST(CASE WHEN dbql.delaytime  >3600                             THEN ReqIOKB/1e6 ELSE 0 END AS INTEGER))) as iogb_in_delaytime_3600_plus


  From pdcrinfo.DBQLogTbl_Hst as dbql
  /* TODO: union with DBQL_Summary table - Paul */
  join dim_user usr
    on dbql.UserName = usr.UserName
  where dbql.LogDate between {startdate} and {enddate}
  Group by
   LogDate
  ,usr.User_Bucket
  ,usr.User_Department
  ,usr.User_SubDepartment
) with data
no primary index on commit preserve rows
;




/* End COA: DBQL_Core */
