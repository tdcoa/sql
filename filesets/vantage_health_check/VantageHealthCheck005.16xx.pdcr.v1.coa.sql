/*
################################################
Query5
- Concurrency Dashboard UX-1
- Query Results File name : consumption_ux_p2_v2
*/


/*{{save:consumption_ux_p2_v2.csv}}*/
Select
 /* Date/Time Columns */
XLogDate
,XLogHour
 /* Request Groupings */
,WorkLoadType
,QueryType
,CASE WHEN StatementType = 'Merge Into' THEN 'Ingest & Prep'
				  WHEN StatementType = 'Begin Loading' THEN 'Ingest & Prep'
				  WHEN StatementType = 'Mload' THEN 'Ingest & Prep'
				  WHEN StatementType = 'Collect Statistics' THEN 'Data Maintenance'
				  WHEN StatementType = 'Delete' THEN 'Ingest & Prep'
				  WHEN StatementType = 'End Loading' THEN 'Ingest & Prep'
				  WHEN StatementType = 'Begin Delete Mload' THEN 'Ingest & Prep'
				  WHEN StatementType = 'Update' THEN 'Ingest & Prep'
				  WHEN StatementType = 'Select' THEN 'Answers'
				  WHEN StatementType = 'Exec' THEN 'Ingest & Prep'
				  WHEN StatementType = 'Release Mload' THEN 'Ingest & Prep'
				  WHEN StatementType = 'Insert' THEN 'Ingest & Prep'
				  WHEN StatementType = 'Begin Mload' THEN 'Ingest & Prep'
				  WHEN StatementType = 'Execute Mload' THEN 'Ingest & Prep'
				  WHEN StatementType = 'Commit Work' THEN 'Ingest & Prep'
				  ELSE 'System/Procedural'
END AS StatementOutcome
,QueryOrigin
,DelaySeconds_Class as "Delay Seconds Group"
,Parse_Time_Class as "Parse Time Group"
,Execution_Time_Class as "Execution Time Group"
,Transfer_Time_Class as "Transfer Time Group"
,AMPCPUTime_Class  as "CPU Group"
,ParserCPUTime_Class as "Parse CPU Group"
,TotalIOCount_Class as "I/O Group"
--,IO_Optimization as "I/O Optimization"
,CacheMissIOPSScore as "Cache Miss Count Score"
,CacheMissKBScore   as "Cache Miss Volume Score"
,Complexity_Effect as "Complexity Effect"
--,COMPLEXITY_CPU
--,COMPLEXITY_IO
 /* Concurrency */
,AVG(XC1) as Concurrency_1Minute_Avg_XC1
,AVG(XC2) as Concurrency_1Minute_Avg_XC2
--,SUM(XC2) over (partition by XlogDate,XLogHour) as Concurrency_Hourly_Avg
,SUM(XC1) as Concurrency_Hourly_Avg_XC1
,SUM(XC2) as Concurrency_Hourly_Avg_XC2
--,AVG(XC2) over (partition by XlogDate,XLogHour,XLogMinute/10) as Concurrency_10Minute_Avg
--,SUM(XC2) over (partition by XlogDate,XLogHour,XLogMinute) as Concurrency_01Minute_Total
 /* Request CPU */
,SUM(XAmpCPUTime) as "X Amp CPUTime"
,SUM(XImpactCPU) as "X Impact CPU"
 /* Request Logical I/O  */
,SUM(XTotalIOCount) as "X Total IO Count"
,SUM(XTotalKio) as "X Total Kio"
,SUM(XSumKIO) as "X Sum KIO"
,SUM(XImpactIO) as "X Impact IO"
,SUM(XReqIOKB) as "X Req IOKB"
,SUM(XSumLogIO_GB) as "XSumLogIO GB"
,SUM(XLogicalIO_GB) as "XLogicalIO GB"
 /* Request Physical I/O  */
,SUM(XReqPhysIO)   as "X Req Phys IO"
,SUM(XPhysKioCnt) as "X Phys Kio Cnt"
,SUM(XSumPhysKioCnt) as "X Sum Phys Kio Cnt"
,SUM(XReqPhysIOKB)  as "X Req Phys IOKB"
,SUM(XSumPhysIO_GB)  as "XSumPhysIO GB"
,SUM(XPhysIO_GB) as "XPhysIO GB"

--,AVG(AVGDelayTime)  - AVG over QueryID for ClockTick
--,AVG(RespTime) - AVG over QueryId for ClockTick
--,AVG(AVG_Execution_Time_Secs)   - AVG over QueryId for ClockTick
--,AVG(AVG_Transfer_Time_Secs)  - AVG over QueryId for ClockTick
--,AVG(AVG_Parse_Time_Secs)  - AVG over QueryId for ClockTick
 /* Request Characteristics */
--,AVG(AVGServerByteCount)    as AvgServerByteCount
--,AVG(AVGCacheMissPctIOPS) as AvgCacheMissPctIOPS
--,AVG(AVGCacheMissPctGB) as AvgCacheMissPctGB
from
(
Select
 /* Date/Time Columns */
 Cast(ClockTick as DATE) as XLogDate
,EXTRACT(HOUR from ClockTick) as XLogHour
,EXTRACT(MINUTE from ClockTick) as XLogMinute
 /* Request Groupings */
,WorkLoadType
,QueryType
,StatementType
,QueryOrigin
,COMPLEXITY_CPU
,COMPLEXITY_IO
,COMPLEXITY_Effect
,DelaySeconds_Class
,Parse_Time_Class
,AMPCPUTime_Class
,ParserCPUTime_Class
,TotalIOCount_Class
,Execution_Time_Class
,Transfer_Time_Class
,IO_Optimization
,CacheMissIOPSScore
,CacheMissKBScore
--,AVG(XC2) over (partition by XlogDate,XLogHour) as Concurrency_Hourly_Avg
--,AVG(XC2) over (partition by XlogDate,XLogHour,XLogMinute/10) as Concurrency_10Minute_Avg
--,SUM(XC2) over (partition by XlogDate,XLogHour,XLogMinute) as Concurrency_01Minute_Total
,SUM(XConcurrency) as XC1
,Count (distinct QueryID) as XC2
 /* Request CPU */
,SUM(AmpCpuTime *  PctSpread) as XAmpCPUTime
,SUM(ImpactCpu  *  PctSpread) as XImpactCPU
 /* Request Logical I/O  */
,SUM(TotalIOCount *  PctSpread) as XTotalIOCount
,SUM(TotalKio *  PctSpread) as XTotalKio
,SUM(SumKio *  PctSpread) as XSumKIO
,SUM(ImpactKio *  PctSpread) as XImpactIO
,SUM(ReqIOKB *  PctSpread) as XReqIOKB
,SUM(SumLogIO_GB *  PctSpread) as XSumLogIO_GB
,SUM(LogicalIO_GB *  PctSpread) as XLogicalIO_GB
 /* Request Physical I/O  */
,SUM(ReqPhysIO *  PctSpread) as XReqPhysIO
,SUM(PhysKioCnt *  PctSpread) as XPhysKioCnt
,SUM(SumPhysKioCnt *  PctSpread) as XSumPhysKioCnt
,SUM(ReqPhysIOKB *  PctSpread) as XReqPhysIOKB
,SUM(SumPhysIO_GB *  PctSpread) as XSumPhysIO_GB
,SUM(PhysIO_GB *  PctSpread) as XPhysIO_GB

--,AVG(DelayTime) as AvgDelayTime - AVG over QueryID for ClockTick
--,AVG(RespTime) - AVG over QueryId for ClockTick
--,AVG(Execution_Time_Secs) as AVG_Execution_Time_Secs  - AVG over QueryId for ClockTick
--,AVG(Transfer_Time_Secs) as Avg_Transfer_Time_secs - AVG over QueryId for ClockTick
--,AVG(Parse_Time_Secs)  as Avg_Parse_Time_Secs - AVG over QueryId for ClockTick
 /* Request Characteristics */
--,AVG(TotalServerByteCount)  as AvgServerByteCount
--,AVG(CacheMissPctIOPS) as AvgCacheMissPctIOPS
--,AVG(CacheMissPctGB) as AvgCacheMissPctGB


From (
select
  QueryID
, firststeptime
, firstresptime as EndTime
, ClockTick
, QryPeriod
, ClockTick - firststeptime day to second as SMinSecs
, ClockTick - EndTime day to second as EMinSecs
, extract (second from SMinSecs) + (extract(minute from SMinSecs)*60) + (extract(hour from SMinSecs)*60*60) + (extract(day from SMinSecs)*86400) as Seconds1
, extract (second from EMinSecs) + (extract(minute from EMinSecs)*60) + (extract(hour from EMinSecs)*60*60) + (extract(day from EMinSecs)*86400) as Seconds2
, CASE
      WHEN Seconds1 < 60 and Seconds2 > 0 and Seconds2 < 60 then Seconds1 - Seconds2 --request entirely within one clocktick
      WHEN Seconds1 < 60 THEN Seconds1 --portion of request before first clocktick
      WHEN Seconds2 >0 and Seconds2 < 60 THEN 60 - Seconds2 --portion of request in last clocktick
  ELSE 60 END as Seconds3  --request active for entire clocktick
, CAST(seconds3 as decimal(9,1)) as Seconds4
, QrySecs  QryRunSecs
, (ampcputime (DECIMAL(18,4)))  TotalQryCpu
, CASE WHEN (seconds4/(QryRunSecs (DECIMAL(38,6)))) = 0 THEN TotalQryCpu
       ELSE (seconds4/(QryRunSecs (DECIMAL(38,6)))) END as PctSpread --Percent of resources applied in period
, (ampcputime)* PctSpread  ClockTickCPU
,QrySecs
,1 as XConcurrency
 /* Date/Time Columns */
,LogDate
,StartDate
,LogHour
,DelayTime
,starttime
,firstresptime
,RespTime
,Execution_Time
,Parse_Time
,Transfer_Time
,Execution_Time_Secs
,Transfer_Time_Secs
,Parse_Time_Secs
,DayOfWeek
 /* Request Characteristics */
,UserName
,AcctString
,AppID
,NumSteps
,NumStepswPar
,MaxStepsInPar
,NumOfActiveAMPs
,Total_AMPs
,StatementType
,TotalServerByteCount
 /* Request CPU */
,AmpCpuTime
,ImpactCpu
 /* Request Logical I/O  */
,TotalIOCount
,TotalKio
,SumKio
,ImpactKio
,ReqIOKB
,SumLogIO_GB
,LogicalIO_GB
 /* Request Physical I/O  */
,ReqPhysIO
,PhysKioCnt
,SumPhysKioCnt
,ReqPhysIOKB
,SumPhysIO_GB
,PhysIO_GB
,CacheMissPctIOPS
,CacheMissPctGB
,ParserCPUTime
,CacheFlag
 /* 30-Day Max CPU & I/O  */
,MAXCPU   --Observed CPU Cieling
,MAXIO  --Observed I/O Ceiling
 /* Request Groupings */
,WorkLoadType
,QueryType
,QueryOrigin
,COMPLEXITY_CPU
,COMPLEXITY_IO
,COMPLEXITY_Effect
,DelaySeconds_Class
,Parse_Time_Class
,AMPCPUTime_Class
,ParserCPUTime_Class
,TotalIOCount_Class
,Execution_Time_Class
,Transfer_Time_Class
,IO_Optimization
,CacheMissIOPSScore
,CacheMissKBScore

 From (
SELECT
 /* EXPAND Execution Time (firststeptime,RespTime): 1 row for each 1-minute interval of execution time */
             BEGIN(Qper)  ClockTick
            ,PERIOD(firststeptime,RespTime) QryPeriod
            ,(CAST( EXTRACT (SECOND FROM FirstRespTime) + (EXTRACT (MINUTE FROM FirstRespTime) * 60 )
            +(EXTRACT (HOUR FROM FirstRespTime) *60*60 ) + (86400 * (CAST ( FirstRespTime AS DATE)
            -CAST ( firststeptime AS DATE) ) )  - (EXTRACT (SECOND FROM firststeptime)
            +(EXTRACT (MINUTE FROM firststeptime) * 60 ) + (EXTRACT (HOUR FROM firststeptime) *60*60 ) ) AS decimal(18,4)) ) QrySecs
 /* Date/Time Columns */
            ,QryLog.LogDate
            ,cast(QryLog.FirstStepTime as DATE) as StartDate
            ,EXTRACT(HOUR FROM QryLog.StartTime) AS LogHour
            ,QryLog.DelayTime
            ,QryLog.starttime
            ,QryLog.firststeptime
            ,QryLog.firstresptime
            ,firstresptime + INTERVAL '59.999999' SECOND as RespTime
            ,((QryLog.FirstRespTime - QryLog.StartTime) HOUR(3) TO SECOND(6)) AS Execution_Time
            ,((QryLog.FirstStepTime - QryLog.StartTime) HOUR(3) TO SECOND(6)) AS Parse_Time
            ,((COALESCE(QryLog.LastRespTime,QryLog.FirstRespTime) - QryLog.FirstRespTime) HOUR(3) TO SECOND(6)) AS Transfer_Time
            ,ZEROIFNULL(CAST(EXTRACT(HOUR FROM Execution_Time) * 3600 + EXTRACT(MINUTE FROM Execution_Time) * 60 + EXTRACT(SECOND FROM Execution_Time) AS FLOAT)) AS Execution_Time_Secs
            ,ZEROIFNULL(CAST(EXTRACT(HOUR FROM Transfer_Time) * 3600 + EXTRACT(MINUTE FROM Transfer_Time) * 60 + EXTRACT(SECOND FROM Transfer_Time) AS FLOAT)) AS Transfer_Time_Secs
            ,ZEROIFNULL(CAST(EXTRACT(HOUR FROM Parse_Time) * 3600 + EXTRACT(MINUTE FROM Parse_Time) * 60 + EXTRACT(SECOND FROM Parse_Time) AS FLOAT)) AS Parse_Time_Secs
            ,CASE QryCal.day_of_week
                WHEN 1 THEN 'Sunday'
                WHEN 2 THEN 'Monday'
                WHEN 3 THEN 'Tuesday'
                WHEN 4 THEN 'Wednesday'
                WHEN 5 THEN 'Thursday'
                WHEN 6 THEN 'Friday'
                WHEN 7 THEN 'Saturday'
             END AS DayOfWeek

 /* Request Characteristics */
            ,QryLog.UserName
            ,QryLog.AcctString
            ,QryLog.AppID
            ,QryLog.NumSteps
            ,QryLog.NumStepswPar
            ,QryLog.MaxStepsInPar
            ,QryLog.NumOfActiveAMPs
            ,HASHAMP() + 1 AS Total_AMPs
            ,QryLog.QueryID
            ,QryLog.StatementType
            ,QryLog.TotalServerByteCount

 /* Request CPU */
            ,QryLog.AmpCpuTime
            ,(QryLog.maxampcputime * QryLog.numofactiveamps) ImpactCpu

 /* Request Logical I/O  */
            ,QryLog.TotalIOCount
            ,QryLog.totaliocount/1000 TotalKio
            ,QryLog.totaliocount/1000 SumKio
            ,(QryLog.maxampio *QryLog.numofactiveamps )/1000 ImpactKio
            ,QryLog.ReqIOKB
            ,QryLog.ReqIOKB/1e6 SumLogIO_GB
            ,QryLog.ReqIOKB/1e6 LogicalIO_GB

 /* Request Physical I/O  */
            ,QryLog.ReqPhysIO
            ,QryLog.ReqPhysIO/1000 PhysKioCnt
            ,QryLog.ReqPhysIO/1000  SumPhysKioCnt
            ,QryLog.ReqPhysIOKB
            ,QryLog.ReqPhysIOKB/1e6 SumPhysIO_GB
            ,QryLog.ReqPhysIOKB/1e6 PhysIO_GB
            ,zeroifnull( SumPhysKioCnt/nullifzero(SumKio) )  CacheMissPctIOPS
            ,zeroifnull(SumPhysIO_GB/nullifzero(SumLogIO_GB))  CacheMissPctGB
            ,QryLog.ParserCPUTime
            ,QryLog.CacheFlag

 /* 30-Day Max CPU & I/O  */
            ,(select MAX(QryLogX.AMPCPUTime) FROM PDCRINFO.DBQLogTbl_Hst QryLogX WHERE LogDate BETWEEN {startdate} AND {enddate} AND StartTime IS NOT NULL) as MAXCPU   --Observed CPU Cieling
            ,(select MAX(QryLogX.TotalIOCount) FROM PDCRINFO.DBQLogTbl_Hst QryLogX WHERE LogDate BETWEEN {startdate} AND {enddate} AND StartTime IS NOT NULL) as MAXIO  --Observed I/O Ceiling

 /* Request Groupings */
            ,CASE
                WHEN QryLog.AppID LIKE ANY('TPTLOAD%', 'TPTUPD%', 'FASTLOAD%', 'MULTLOAD%', 'EXECUTOR%', 'JDBCL%')        THEN 'LOAD'
                WHEN QryLog.StatementType IN ('Insert', 'Update', 'Delete', 'Create Table', 'Merge Into')
                 AND QryLog.AppID NOT LIKE ANY('TPTLOAD%', 'TPTUPD%', 'FASTLOAD%', 'MULTLOAD%', 'EXECUTOR%', 'JDBCL%')    THEN 'ETL/ELT'
                WHEN QryLog.StatementType = 'Select' AND (AppID IN ('TPTEXP', 'FASTEXP') or appid like  'JDBCE%')         THEN 'EXPORT'
                WHEN QryLog.StatementType = 'Select'
                    AND QryLog.AppID NOT LIKE ANY('TPTLOAD%', 'TPTUPD%', 'FASTLOAD%', 'MULTLOAD%', 'EXECUTOR%', 'JDBCL%') THEN 'QUERY'
                WHEN QryLog.StatementType in ('Dump Database','Unrecognized type','Release Lock','Collect Statistics')    THEN 'ADMIN'
                                                                                                                          ELSE 'OTHER'
            END AS WorkLoadType
            ,CASE
                WHEN StatementType = 'Select'
                    AND AppID NOT IN ('TPTEXP', 'FASTEXP')
                    AND Execution_Time_Secs < 1
                    AND NumOfActiveAMPs < Total_AMPs
                    THEN 'Tactical'
                ELSE
                    'Non-Tactical'
            END AS QueryType
            ,CASE
                WHEN TotalServerByteCount > 0 THEN 'QueryGrid'
                                              ELSE 'Local'
             END AS QueryOrigin
            ,CASE
                WHEN QryLog.AMPCPUTime BETWEEN ((MAXCPU/10)*0)  and ((MAXCPU/10)*1) THEN 0
                WHEN QryLog.AMPCPUTime BETWEEN ((MAXCPU/10)*1)  and ((MAXCPU/10)*2) THEN 1
                WHEN QryLog.AMPCPUTime BETWEEN ((MAXCPU/10)*2)  and ((MAXCPU/10)*3) THEN 2
                WHEN QryLog.AMPCPUTime BETWEEN ((MAXCPU/10)*3)  and ((MAXCPU/10)*4) THEN 3
                WHEN QryLog.AMPCPUTime BETWEEN ((MAXCPU/10)*4)  and ((MAXCPU/10)*5) THEN 4
                WHEN QryLog.AMPCPUTime BETWEEN ((MAXCPU/10)*5)  and ((MAXCPU/10)*6) THEN 5
                WHEN QryLog.AMPCPUTime BETWEEN ((MAXCPU/10)*6)  and ((MAXCPU/10)*7) THEN 6
                WHEN QryLog.AMPCPUTime BETWEEN ((MAXCPU/10)*7)  and ((MAXCPU/10)*8) THEN 7
                WHEN QryLog.AMPCPUTime BETWEEN ((MAXCPU/10)*8)  and ((MAXCPU/10)*9) THEN 8
                WHEN QryLog.AMPCPUTime BETWEEN ((MAXCPU/10)*9)  and ((MAXCPU/10)*10) THEN 9
                WHEN QryLog.AMPCPUTime > ((MAXCPU/10)*10) THEN 10
             END as COMPLEXITY_CPU
            ,CASE
                WHEN QryLog.TotalIOCount BETWEEN ((MAXIO/10)*0)  and ((MAXIO/10)*1) THEN 0
                WHEN QryLog.TotalIOCount BETWEEN ((MAXIO/10)*1)  and ((MAXIO/10)*2) THEN 1
                WHEN QryLog.TotalIOCount BETWEEN ((MAXIO/10)*2)  and ((MAXIO/10)*3) THEN 2
                WHEN QryLog.TotalIOCount BETWEEN ((MAXIO/10)*3)  and ((MAXIO/10)*4) THEN 3
                WHEN QryLog.TotalIOCount BETWEEN ((MAXIO/10)*4)  and ((MAXIO/10)*5) THEN 4
                WHEN QryLog.TotalIOCount BETWEEN ((MAXIO/10)*5)  and ((MAXIO/10)*6) THEN 5
                WHEN QryLog.TotalIOCount BETWEEN ((MAXIO/10)*6)  and ((MAXIO/10)*7) THEN 6
                WHEN QryLog.TotalIOCount BETWEEN ((MAXIO/10)*7)  and ((MAXIO/10)*8) THEN 7
                WHEN QryLog.TotalIOCount BETWEEN ((MAXIO/10)*8)  and ((MAXIO/10)*9) THEN 8
                WHEN QryLog.TotalIOCount BETWEEN ((MAXIO/10)*9)  and ((MAXIO/10)*10) THEN 9
                WHEN QryLog.TotalIOCount > ((MAXIO/10)*10) THEN 10
            END as COMPLEXITY_IO
            ,(((COMPLEXITY_CPU + COMPLEXITY_IO +0.5)/2) (DECIMAL(6,0))) as COMPLEXITY_Effect
            ,CASE
                WHEN DelayTime is NULL                      THEN '0000 - 0000'
                WHEN DelayTime < 1.0 or DelayTime is NULL   THEN '0000 - 0001'
                WHEN DelayTime BETWEEN 1.0 AND 5.0          THEN '0001 - 0005'
                WHEN DelayTime BETWEEN 5.0 AND 10.0         THEN '0005 - 0010'
                WHEN DelayTime BETWEEN 10.0 AND 30.0        THEN '0010 - 0030'
                WHEN DelayTime BETWEEN 30.0 AND 60.0        THEN '0030 - 0060'
                WHEN DelayTime BETWEEN 60.0 AND 300.0       THEN '0060 - 0300'
                WHEN DelayTime BETWEEN 300.0 AND 600.0      THEN '0300 - 0600'
                WHEN DelayTime BETWEEN 600.0 AND 1800.0     THEN '0600 - 1800'
                WHEN DelayTime BETWEEN 1800.0 AND 3600.0    THEN '1800 - 3600'
                WHEN DelayTime > 3600.0                     THEN '3600+'
             END AS DelaySeconds_Class
            ,CASE
                WHEN Parse_Time_Secs IS NULL                    THEN '0000 - 0000'
                WHEN Parse_Time_Secs < 1.0                      THEN '0000 - 0001'
                WHEN Parse_Time_Secs BETWEEN 1.0 AND 5.0        THEN '0001 - 0005'
                WHEN Parse_Time_Secs BETWEEN 5.0 AND 10.0       THEN '0005 - 0010'
                WHEN Parse_Time_Secs BETWEEN 10.0 AND 30.0      THEN '0010 - 0030'
                WHEN Parse_Time_Secs BETWEEN 30.0 AND 60.0      THEN '0030 - 0060'
                WHEN Parse_Time_Secs BETWEEN 60.0 AND 300.0     THEN '0060 - 0300'
                WHEN Parse_Time_Secs BETWEEN 300.0 AND 600.0    THEN '0300 - 0600'
                WHEN Parse_Time_Secs BETWEEN 600.0 AND 1800.0   THEN '0600 - 1800'
                WHEN Parse_Time_Secs BETWEEN 1800.0 AND 3600.0  THEN '1800 - 3600'
                WHEN Parse_Time_Secs > 3600.0                   THEN '3600+'
             END AS Parse_Time_Class
            ,CASE
                WHEN AMPCPUTime IS NULL                        THEN '00000 - 000000'
		WHEN AMPCPUTime < 1.0                          THEN '00000 - 000001'
                WHEN AMPCPUTime BETWEEN 1.0 AND 10.0           THEN '00001 - 000010'
                WHEN AMPCPUTime BETWEEN 10.0 AND 100.0         THEN '00010 - 000100'
                WHEN AMPCPUTime BETWEEN 100.0 AND 1000.0       THEN '00100 - 001000'
                WHEN AMPCPUTime BETWEEN 1000.0 AND 10000.0     THEN '01000 - 010000'
                WHEN AMPCPUTime BETWEEN 10000.0 AND 100000.0   THEN '10000 - 100000'
                WHEN AMPCPUTime > 100000.0                     THEN '100000+'
            END AS AMPCPUTime_Class
            ,CASE
                WHEN ParserCPUTime IS NULL                     THEN '00000 - 00000'
                WHEN ParserCPUTime < 1.0                       THEN '00000 - 00001'
                WHEN ParserCPUTime BETWEEN 1.0 AND 5.0         THEN '00001 - 00005'
                WHEN ParserCPUTime BETWEEN 5.0 AND 10.0        THEN '00005 - 00010'
                WHEN ParserCPUTime BETWEEN 10.0 AND 50.0       THEN '00010 - 00050'
                WHEN ParserCPUTime BETWEEN 50.0 AND 100.0      THEN '00050 - 00100'
                WHEN ParserCPUTime BETWEEN 100.0 AND 500.0     THEN '00100 - 00500'
                WHEN ParserCPUTime BETWEEN 500.0 AND 1000.0    THEN '00500 - 01000'
                WHEN ParserCPUTime BETWEEN 1000.0 AND 5000.0   THEN '01000 - 05000'
                WHEN ParserCPUTime BETWEEN 5000.0 AND 10000.0  THEN '05000 - 10000'
                WHEN ParserCPUTime > 10000.0                   THEN '10000+'
             END AS ParserCPUTime_Class
            ,CASE
                WHEN TotalIOCount IS NULL                 THEN '1e0-1e0'
                WHEN TotalIOCount < 1e4                   THEN '1e0-1e4'
                WHEN TotalIOCount BETWEEN 1e4 AND 1e6     THEN '1e4-1e6'
                WHEN TotalIOCount BETWEEN 1e6 AND 1e8     THEN '1e6-1e8'
                WHEN TotalIOCount BETWEEN 1e8 AND 1e10    THEN '1e8-1e10'
                WHEN TotalIOCount > 1e10                  THEN '1e10+'
             END AS TotalIOCount_Class
            ,CASE
                WHEN Execution_Time_Secs IS NULL                   THEN '00000 - 000000'
		WHEN Execution_Time_Secs < 1.0                     THEN '00000 - 000001'
                WHEN Execution_Time_Secs BETWEEN 1.0 AND 1e1       THEN '00001 - 000010'
                WHEN Execution_Time_Secs BETWEEN 1e1 AND 1e2       THEN '00010 - 000100'
                WHEN Execution_Time_Secs BETWEEN 1e2 AND 1e3       THEN '00100 - 001000'
                WHEN Execution_Time_Secs BETWEEN 1e3 AND 1e4       THEN '01000 - 010000'
                WHEN Execution_Time_Secs > 1e4                     THEN '10000+'
            END AS Execution_Time_Class
            ,CASE
                WHEN Transfer_Time_Secs IS NULL                   THEN '0000 - 0000'
                WHEN Transfer_Time_Secs < 1.0                     THEN '0000 - 0001'
                WHEN Transfer_Time_Secs BETWEEN 1.0 AND 5.0       THEN '0001 - 0005'
                WHEN Transfer_Time_Secs BETWEEN 5.0 AND 10.0      THEN '0005 - 0010'
                WHEN Transfer_Time_Secs BETWEEN 10.0 AND 30.0     THEN '0010 - 0030'
                WHEN Transfer_Time_Secs BETWEEN 30.0 AND 60.0     THEN '0030 - 0060'
                WHEN Transfer_Time_Secs BETWEEN 60.0 AND 300.0    THEN '0060 - 0300'
                WHEN Transfer_Time_Secs BETWEEN 300.0 AND 600.0   THEN '0300 - 0600'
                WHEN Transfer_Time_Secs BETWEEN 600.0 AND 1800.0  THEN '0600 - 1800'
                WHEN Transfer_Time_Secs BETWEEN 1800.0 AND 3600.0 THEN '1800 - 3600'
                WHEN Transfer_Time_Secs > 3600.0                  THEN '3600 - 9999'
            END AS Transfer_Time_Class

            ,CASE
                WHEN TotalIOCount = 0
                    THEN 'No I/O'
                WHEN TotalIOCount > 0 AND ReqPhysIO = 0
                    THEN 'In Memory'
                WHEN TotalIOCount > 0 AND ReqPhysIO > 0
                    THEN 'Physical I/O'
             END AS IO_Optimization

 /* Cache Miss Rate IOPS.  normal cache miss rate <20%,   set score = 0  for  miss rate < 20%,  increments of 10%, range 0 -80 */
            ,case
                when  SumPhysKioCnt = 0 then 0
                when   zeroifnull(SumPhysKioCnt/ nullifzero(SumKio)) <= 0.20 then 0                         /* set score = 0 when less than industry average 20% */
                when   SumPhysKioCnt > SumKio then 80                                                       /* sometimes get Physical > Logical, set ceiling at 80*/
                else (cast( 100 * zeroifnull (SumPhysKioCnt/ nullifzero(SumKio)) /10 as  integer) * 10) - 20  /* only count above 20%, round to bin size 10*/
            end as CacheMissIOPSScore

 /* Cache Miss Rate KB.  normal cache miss rate <20%,   set score = 0  for  miss rate < 20%,  increments of 10%, range 0 -80 */
            ,case
                when  SumPhysIO_GB = 0 then 0
                when   zeroifnull(SumPhysIO_GB/ nullifzero(SumLogIO_GB)) <= 0.20 then 0                   /* set score = 0 when less than industry average 20% */
                when   SumPhysIO_GB > SumLogIO_GB then 80                                  /* sometimes get Physical > Logical, set ceiling at 80*/
                else  (cast( 100 * zeroifnull (SumPhysIO_GB/ nullifzero(SumLogIO_GB)) /10 as  integer) * 10) - 20   /* only count above 20%, round to bin size 10*/
            end as CacheMissKBScore

        FROM
            PDCRINFO.DBQLogTbl_Hst QryLog
            INNER JOIN
            Sys_Calendar.CALENDAR QryCal
                ON QryCal.calendar_date = QryLog.LogDate
        WHERE
            LogDate BETWEEN {startdate} AND {enddate}
            --AND Extract (HOUR from StartTime) in (8,9,10)
            AND StartTime IS NOT NULL
            AND AMPCPUTime > 0
            AND QrySecs > 0
        EXPAND ON QryPeriod AS QPer BY ANCHOR ANCHOR_MINUTE

)  x1
--where PctCPU < 1
)  x2
Group By 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
)  x3
Group By 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
-- Order by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
;
