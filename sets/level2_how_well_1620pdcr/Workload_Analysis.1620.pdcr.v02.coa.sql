/*
Parameters:
  {dbqlogtbl} = PDCRINFO.DBQLogTbl_Hst
  {siteid}
  {startdate}
  {enddate}
*/

/*{{save:adlste_westcomm.consumption_UX_P1.csv}}*/
/*{{load:adlste_westcomm.consumption_UX_P1_stg}}*/
/*{{call:adlste_westcomm.consumption_UX_P1_sp('{fileset_version}')}}*/

SELECT
     '{siteid}'  as SiteID
	,LogDate
    ,LogHour
    ,WorkLoadType as "Workload Type"
    ,QueryOrigin as "Query Origin"
    ,DelaySeconds_Class as "Delay Seconds Group"
	,Parse_Time_Class as "Parse Time Group"
    ,Execution_Time_Class as "Execution Time Group"
    ,Transfer_Time_Class as "Transfer Time Group"
	,AMPCPUTime_Class  as "CPU Group"
	,ParserCPUTime_Class as "Parse CPU Group"
	,TotalIOCount_Class as "I/O Group"

	,CacheMissIOPSScore as "Cache Miss Count Score"
	,CacheMissKBScore   as "Cache Miss Volume Score"
	,Complexity_Effect as "Complexity Effect"
    ,COUNT(*) AS "Request Count"
    ,SUM(AMPCPUTime) AS "Total AMPCPUTime"
    ,SUM(TotalIOCount) AS "Total IOCount"
	,SUM(ReqIOKB) AS "Total ReqIOKB"
    ,SUM(ReqPhysIO) AS "Total ReqPhysIO"
    ,SUM(ReqPhysIOKB) AS "Total ReqPhysIOKB"
	,SUM(TotalServerByteCount) AS "Total Server Byte Count"
FROM
    (
        SELECT
            QryLog.LogDate
            ,EXTRACT(HOUR FROM StartTime) AS LogHour
            ,
            CASE QryCal.day_of_week
                WHEN 1
                    THEN 'Sunday'
                WHEN 2
                    THEN 'Monday'
                WHEN 3
                    THEN 'Tuesday'
                WHEN 4
                    THEN 'Wednesday'
                WHEN 5
                    THEN 'Thursday'
                WHEN 6
                    THEN 'Friday'
                WHEN 7
                    THEN 'Saturday'
            END AS DayOfWeek
            ,QryLog.UserName
            ,QryLog.AcctString
            ,QryLog.AppID
			,QryLog.NumSteps
			,QryLog.NumStepswPar
            ,QryLog.MaxStepsInPar
            ,HASHAMP() + 1 AS Total_AMPs
            ,QryLog.QueryID
            ,QryLog.StatementType
            ,
            CASE
                WHEN (QryLog.StatementType IN ('Insert', 'Update', 'Delete', 'Create Table', 'Merge Into')
					OR QryLog.AppID LIKE ANY('TPTLOAD%', 'TPTUPD%', 'FASTLOAD%', 'MULTLOAD%', 'EXECUTOR%', 'JDBCL%'))
                    THEN 'ETL/ELT'
                WHEN QryLog.StatementType = 'Select'
                    AND (AppID IN ('TPTEXP', 'FASTEXP') or appid like  'JDBCE%')
                    THEN 'EXPORT'
                WHEN QryLog.StatementType = 'Select'
                    AND QryLog.AppID NOT LIKE ANY('TPTLOAD%', 'TPTUPD%', 'FASTLOAD%', 'MULTLOAD%', 'EXECUTOR%', 'JDBCL%', 'JDBCE%')
                    THEN 'QUERY'
                ELSE
                    'OTHER'
            END AS WorkLoadType
            ,
            CASE
                WHEN StatementType = 'Select'
                    AND AppID NOT IN ('TPTEXP', 'FASTEXP')
                    AND Execution_Time_Secs < 1
                    AND NumOfActiveAMPs < Total_AMPs
                    THEN 'Tactical'
                ELSE
                    'Non-Tactical'
            END AS QueryType
            ,
            CASE
                WHEN TotalServerByteCount > 0
                    THEN 'QueryGrid'
                ELSE
                    'Local'
            END AS QueryOrigin
            ,QryLog.NumOfActiveAMPs
            ,QryLog.AMPCPUTime

            ,QryLog.TotalIOCount
			,QryLog.ReqIOKB
            ,QryLog.ReqPhysIO
            ,QryLog.ReqPhysIOKB
			,QryLog.ParserCPUTime
			,QryLog.CacheFlag
			,QryLog.DelayTime
			,QryLog.TotalServerByteCount
			,(select MAX(QryLog.AMPCPUTime) FROM PDCRINFO.DBQLogTbl_Hst QryLog WHERE LogDate BETWEEN {startdate}  AND {enddate} AND StartTime IS NOT NULL) as MAXCPU
			,(select MAX(QryLog.TotalIOCount) FROM PDCRINFO.DBQLogTbl_Hst QryLog WHERE LogDate BETWEEN {startdate}  AND {enddate} AND StartTime IS NOT NULL) as MAXIO
			,
			CASE
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
			,
			CASE
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
			,
            CASE
                WHEN DelayTime is NULL
                    THEN '0000 - 0000'
				WHEN DelayTime < 1.0 or DelayTime is NULL
                    THEN '0000 - 0001'
                WHEN DelayTime BETWEEN 1.0 AND 5.0
                    THEN '0001 - 0005'
                WHEN DelayTime BETWEEN 5.0 AND 10.0
                    THEN '0005 - 0010'
                WHEN DelayTime BETWEEN 10.0 AND 30.0
                    THEN '0010 - 0030'
                WHEN DelayTime BETWEEN 30.0 AND 60.0
                    THEN '0030 - 0060'
                WHEN DelayTime BETWEEN 60.0 AND 300.0
                    THEN '0060 - 0300'
                WHEN DelayTime BETWEEN 300.0 AND 600.0
                    THEN '0300 - 0600'
                WHEN DelayTime BETWEEN 600.0 AND 1800.0
                    THEN '0600 - 1800'
                WHEN DelayTime BETWEEN 1800.0 AND 3600.0
                    THEN '1800 - 3600'
                WHEN DelayTime > 3600.0
                    THEN '3600+'
            END AS DelaySeconds_Class
            ,((FirstRespTime - StartTime) HOUR(3) TO SECOND(6)) AS Execution_Time
            ,((FirstStepTime - StartTime) HOUR(3) TO SECOND(6)) AS Parse_Time
            ,((COALESCE(LastRespTime,FirstRespTime) - FirstRespTime) HOUR(3) TO SECOND(6)) AS Transfer_Time
            ,ZEROIFNULL(CAST(EXTRACT(HOUR FROM Execution_Time) * 3600 + EXTRACT(MINUTE FROM Execution_Time) * 60 + EXTRACT(SECOND FROM Execution_Time) AS FLOAT)) AS Execution_Time_Secs
            ,ZEROIFNULL(CAST(EXTRACT(HOUR FROM Transfer_Time) * 3600 + EXTRACT(MINUTE FROM Transfer_Time) * 60 + EXTRACT(SECOND FROM Transfer_Time) AS FLOAT)) AS Transfer_Time_Secs
            ,ZEROIFNULL(CAST(EXTRACT(HOUR FROM Parse_Time) * 3600 + EXTRACT(MINUTE FROM Parse_Time) * 60 + EXTRACT(SECOND FROM Parse_Time) AS FLOAT)) AS Parse_Time_Secs
			,
            CASE
                WHEN Parse_Time_Secs IS NULL
                    THEN '0000 - 0000'
				WHEN Parse_Time_Secs < 1.0
                    THEN '0000 - 0001'
                WHEN Parse_Time_Secs BETWEEN 1.0 AND 5.0
                    THEN '0001 - 0005'
                WHEN Parse_Time_Secs BETWEEN 5.0 AND 10.0
                    THEN '0005 - 0010'
                WHEN Parse_Time_Secs BETWEEN 10.0 AND 30.0
                    THEN '0010 - 0030'
                WHEN Parse_Time_Secs BETWEEN 30.0 AND 60.0
                    THEN '0030 - 0060'
                WHEN Parse_Time_Secs BETWEEN 60.0 AND 300.0
                    THEN '0060 - 0300'
                WHEN Parse_Time_Secs BETWEEN 300.0 AND 600.0
                    THEN '0300 - 0600'
                WHEN Parse_Time_Secs BETWEEN 600.0 AND 1800.0
                    THEN '0600 - 1800'
                WHEN Parse_Time_Secs BETWEEN 1800.0 AND 3600.0
                    THEN '1800 - 3600'
                WHEN Parse_Time_Secs > 3600.0
                    THEN '3600+'
            END AS Parse_Time_Class
			,
            CASE
                WHEN AMPCPUTime IS NULL
                    THEN '00000 - 000000'
				WHEN AMPCPUTime < 1.0
                    THEN '00000 - 000001'
                WHEN AMPCPUTime BETWEEN 1.0 AND 10.0
                    THEN '00001 - 000010'
                WHEN AMPCPUTime BETWEEN 10.0 AND 100.0
                    THEN '00010 - 000100'
                WHEN AMPCPUTime BETWEEN 100.0 AND 1000.0
                    THEN '00100 - 001000'
                WHEN AMPCPUTime BETWEEN 1000.0 AND 10000.0
                    THEN '01000 - 010000'
                WHEN AMPCPUTime BETWEEN 10000.0 AND 100000.0
                    THEN '10000 - 100000'
                WHEN AMPCPUTime > 100000.0
                    THEN '100000+'
            END AS AMPCPUTime_Class
			,
            CASE
                WHEN ParserCPUTime IS NULL
                    THEN '00000 - 00000'
				WHEN ParserCPUTime < 1.0
                    THEN '00000 - 00001'
                WHEN ParserCPUTime BETWEEN 1.0 AND 5.0
                    THEN '00001 - 00005'
                WHEN ParserCPUTime BETWEEN 5.0 AND 10.0
                    THEN '00005 - 00010'
                WHEN ParserCPUTime BETWEEN 10.0 AND 50.0
                    THEN '00010 - 00050'
                WHEN ParserCPUTime BETWEEN 50.0 AND 100.0
                    THEN '00050 - 00100'
                WHEN ParserCPUTime BETWEEN 100.0 AND 500.0
                    THEN '00100 - 00500'
                WHEN ParserCPUTime BETWEEN 500.0 AND 1000.0
                    THEN '00500 - 01000'
                WHEN ParserCPUTime BETWEEN 1000.0 AND 5000.0
                    THEN '01000 - 05000'
                WHEN ParserCPUTime BETWEEN 5000.0 AND 10000.0
                    THEN '05000 - 10000'
                WHEN ParserCPUTime > 10000.0
                    THEN '10000+'
            END AS ParserCPUTime_Class
			,
            CASE
                WHEN TotalIOCount IS NULL
                    THEN '1e0-1e0'
				WHEN TotalIOCount < 1e4
                    THEN '1e0-1e4'
                WHEN TotalIOCount BETWEEN 1e4 AND 1e6
                    THEN '1e4-1e6'
                WHEN TotalIOCount BETWEEN 1e6 AND 1e8
                    THEN '1e6-1e8'
                WHEN TotalIOCount BETWEEN 1e8 AND 1e10
                    THEN '1e8-1e10'
                WHEN TotalIOCount > 1e10
                    THEN '1e10+'
            END AS TotalIOCount_Class
            ,
            CASE
                WHEN Execution_Time_Secs IS NULL
                    THEN '00000 - 000000'
				WHEN Execution_Time_Secs < 1.0
                    THEN '00000 - 000001'
                WHEN Execution_Time_Secs BETWEEN 1.0 AND 1e1
                    THEN '00001 - 000010'
                WHEN Execution_Time_Secs BETWEEN 1e1 AND 1e2
                    THEN '00010 - 000100'
                WHEN Execution_Time_Secs BETWEEN 1e2 AND 1e3
                    THEN '00100 - 001000'
                WHEN Execution_Time_Secs BETWEEN 1e3 AND 1e4
                    THEN '01000 - 010000'
                WHEN Execution_Time_Secs > 1e4
                    THEN '10000+'
            END AS Execution_Time_Class
            ,
            CASE
                WHEN TotalIOCount = 0
                    THEN 'No I/O'
				WHEN TotalIOCount > 0 AND ReqPhysIO = 0
                    THEN 'In Memory'
                WHEN TotalIOCount > 0 AND ReqPhysIO > 0
                    THEN 'Physical I/O'
            END AS IO_Optimization


 /*   IOPS  Metrics  */
, (totaliocount)/1000 SumKio
, (ReqPhysIO)/1000  SumPhysKioCnt
, zeroifnull( SumPhysKioCnt/nullifzero(SumKio) )  CacheMissPctIOPS

/*  IO Bytes Metrics  */
, (ReqIOKB)/1e6 SumLogIO_GB
, (ReqPhysIOKB)/1e6 SumPhysIO_GB
, zeroifnull(SumPhysIO_GB/nullifzero(SumLogIO_GB))  CacheMissPctGB

 /* METRIC:   Cache Miss Rate IOPS.  normal cache miss rate <20%,   set score = 0  for  miss rate < 20%,  increments of 10%, range 0 -80 */
			,
			case
				when  SumPhysKioCnt = 0 then 0
                when   zeroifnull(SumPhysKioCnt/ nullifzero(SumKio)) <= 0.20 then 0                         /* set score = 0 when less than industry average 20% */
                when   SumPhysKioCnt > SumKio then 80                                                       /* sometimes get Physical > Logical, set ceiling at 80*/
                else (cast( 100 * zeroifnull (SumPhysKioCnt/ nullifzero(SumKio)) /10 as  integer) * 10) - 20  /* only count above 20%, round to bin size 10*/
            end as CacheMissIOPSScore

 /* METRIC:   Cache Miss Rate KB.  normal cache miss rate <20%,   set score = 0  for  miss rate < 20%,  increments of 10%, range 0 -80 */
   			,
   			case
				when  SumPhysIO_GB = 0 then 0
                when   zeroifnull(SumPhysIO_GB/ nullifzero(SumLogIO_GB)) <= 0.20 then 0                   /* set score = 0 when less than industry average 20% */
                when   SumPhysIO_GB > SumLogIO_GB then 80                                  /* sometimes get Physical > Logical, set ceiling at 80*/
                else  (cast( 100 * zeroifnull (SumPhysIO_GB/ nullifzero(SumLogIO_GB)) /10 as  integer) * 10) - 20   /* only count above 20%, round to bin size 10*/
            end as CacheMissKBScore
            ,
            CASE
                WHEN Transfer_Time_Secs IS NULL
                    THEN '0000 - 0000'
				WHEN Transfer_Time_Secs < 1.0
                    THEN '0000 - 0001'
                WHEN Transfer_Time_Secs BETWEEN 1.0 AND 5.0
                    THEN '0001 - 0005'
                WHEN Transfer_Time_Secs BETWEEN 5.0 AND 10.0
                    THEN '0005 - 0010'
                WHEN Transfer_Time_Secs BETWEEN 10.0 AND 30.0
                    THEN '0010 - 0030'
                WHEN Transfer_Time_Secs BETWEEN 30.0 AND 60.0
                    THEN '0030 - 0060'
                WHEN Transfer_Time_Secs BETWEEN 60.0 AND 300.0
                    THEN '0060 - 0300'
                WHEN Transfer_Time_Secs BETWEEN 300.0 AND 600.0
                    THEN '0300 - 0600'
                WHEN Transfer_Time_Secs BETWEEN 600.0 AND 1800.0
                    THEN '0600 - 1800'
                WHEN Transfer_Time_Secs BETWEEN 1800.0 AND 3600.0
                    THEN '1800 - 3600'
                WHEN Transfer_Time_Secs > 3600.0
                    THEN '3600+'
            END AS Transfer_Time_Class
        FROM
            {dbqlogtbl} QryLog
            INNER JOIN
            Sys_Calendar.CALENDAR QryCal
                ON QryCal.calendar_date = QryLog.LogDate
        WHERE
            LogDate BETWEEN {startdate}  AND {enddate}
            AND StartTime IS NOT NULL
    ) AS QryDetails
GROUP BY
     1
    ,2
    ,3
    ,4
    ,5
    ,6
    ,7
    ,8
    ,9
	,10
	,11
	,12
	,13
	,13
	,14
	,15




order by LogDate, LogHour, "Total AMPCPUTime" desc;
