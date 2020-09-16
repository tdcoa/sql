/* Start COA: COA_Dashboard
   see comments about each SQL step inline below.

Parameters:
  - startdate:    	{startdate}
  - enddate:      	{enddate}
  - siteid:       	{siteid}
  - dbqlogtbl:    	{dbqlogtbl}
  - resusagespma: 	{resusagespma}
  - tcorebudget:  	{tcorebudget}
  - tcorecapacity:	{tcorecapacity}
  - qtroffset:		{qtroffset}
  - systemcost:		{systemcost}
  - dbqlflushrate:	{dbqlflushrate}
  - SPMAInterval:	{spmainterval}
  - AvgMBSecRatio:	{avgmbsecratio}
  - daterange:		{daterange}
*/

/*{{temp:dates.csv}}*/ ;
/*{{temp:consumption_step.csv}}*/ ;
/*{{temp:consumption_stepgrp.csv}}*/ ;
/*{{temp:stepgroupfunctions.csv}}*/ ;

CREATE VOLATILE TABLE CB_DAILY_TCORE_METRICS ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      LOGDATE DATE FORMAT 'YYYY-MM-DD',
      SYSTEM_ID VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      SYSTEM_VERSION VARCHAR(16384) CHARACTER SET LATIN NOT CASESPECIFIC,
      SYSTEM_NODE_CNT INTEGER,
      SYSTEM_TCORE_CAPACITY INTEGER,
 --     HOUR_DT SMALLINT,
      CPU_RATIO DECIMAL(10,5),
      IO_RATIO DECIMAL(10,5),
      TCORE_HRS_DAY INTEGER,
      TCORE_HRS_MTD INTEGER,
      MTD_AVG INTEGER,
      JOB_NM VARCHAR(27) CHARACTER SET UNICODE NOT CASESPECIFIC,
      USR_NM VARCHAR(12) CHARACTER SET UNICODE NOT CASESPECIFIC,
      EDW_START_TSP TIMESTAMP(6) WITH TIME ZONE)
PRIMARY INDEX ( LOGDATE )  ON COMMIT PRESERVE ROWS

;


INSERT INTO CB_DAILY_TCORE_METRICS
SELECT Logdate ,
	System_id,
	system_version,
	numnodes as System_Node_Cnt,
	System_TCore_Capacity,
	--hour_dt,
    CPU_Ratio,
	IO_Ratio,
	CAST(ROUND(TCore_hrs_day,0) as INT) (named "TCore_hrs_day"),
	CAST(ROUND(TCore_hrs_MTD,0) as INT) (named "TCore_hrs_MTD"),
    CAST(ROUND(Daily_Avg,0) as INT) (named "MTD_Avg"),
	'CB_DAILY_TCORE_METRICS_Load',
	'COA_Usr',
	current_timestamp(6)

    FROM

    (SELECT System_id,
    system_version,
    numnodes,
    System_TCore_Capacity,
    Logdate,
	hour_dt,
    TCore_hrs_day,
	CPU_Ratio,
	IO_Ratio,
    TCore_hrs_MTD/EXTRACT(DAY
    FROM logdate) AS Daily_Avg,
    SUM(TCore_hrs_day) OVER (ORDER BY logdate ROWS UNBOUNDED PRECEDING) AS TCore_hrs_MTD
    FROM

    (SELECT System_id,
    system_version,
    System_TCore_Capacity,
    numnodes,
    Logdate,
	hour_dt,
	sum(TCore_CPU_cnt)/(sum(TCore_CPU_cnt) + sum(TCore_IO_cnt)) as CPU_Ratio,
	sum(TCore_IO_cnt)/(sum(TCore_CPU_cnt) + sum(TCore_IO_cnt)) as IO_Ratio,
--  SUM(normalized_tcore_consumed)/6  AS TCore_hrs_day	--Replace 6 (Hardcode) into lookup table
    SUM(normalized_tcore_consumed)/{dbqlflushrate}  AS TCore_hrs_day
    FROM

    (SELECT
    CAST(core_dt.System_ID AS VARCHAR(100)) (NAMED "System_ID")
    ,core_dt.NumNodes
    ,core_dt.System_Version
    ,core_dt.LogDate
    ,core_dt.Timestamp_dt
    ,core_dt.Hour_dt
    ,CAST(core_dt.AvgCPUBusy/100 AS FLOAT) (NAMED "Percent_Compute_consumed")
--  ,CAST(core_dt.AvgMBSecNode/3122 AS FLOAT) (NAMED "Percent_IO_consumed") --Replace 3122 (Hardcode) into lookup table
    ,CAST(core_dt.AvgMBSecNode/{avgmbsecratio} AS FLOAT) (NAMED "Percent_IO_consumed")
    ,CAST(System_TCore_Capacity AS INT) (NAMED "System_TCore_Capacity")
    ,CAST(CEILING(System_TCore_Capacity * Percent_Compute_consumed) AS INT) (NAMED "TCore_CPU_consumed")
    ,CAST(CEILING(System_TCore_Capacity * Percent_IO_consumed) AS INT) (NAMED "TCore_IO_consumed")
    ,CAST(GREATEST(TCore_CPU_consumed,TCore_IO_consumed) AS INT) (NAMED "TCore_consumed")
    ,case when ( TCore_CPU_consumed > TCore_IO_consumed ) then 1.00000 else 0.00000 end as TCore_CPU_cnt
	,case when ( TCore_CPU_consumed <= TCore_IO_consumed ) then 1.00000 else 0.00000 end as TCore_IO_cnt

    ,CASE
        WHEN TCore_consumed >= System_TCore_Capacity THEN System_TCore_Capacity
    ELSE TCore_consumed
    END AS Normalized_TCore_consumed
    FROM
    (
    SELECT
        /* Time Data */

    spma_dt.LogDate (NAMED "LogDate")
    ,CAST((spma_dt.LogDate || ' ' || spma_dt.LogTime) AS TIMESTAMP(0)) (NAMED "Timestamp_dt")
    ,EXTRACT(HOUR
    FROM "Timestamp_dt") (NAMED "Hour_dt")
    ,SPMAInterval (NAMED "RSSInterval")

        /* System data */

    ,'{siteid}' (NAMED "System_ID")
    ,info.infodata (NAMED "System_Version")
    ,'{tcorecapacity}' (NAMED "System_TCore_Capacity")
    ,spma_dt.NCPUs (NAMED "CPUs")
    ,COUNT(DISTINCT(spma_dt.NodeID)) (NAMED "NumNodes")

        /* CPU/IO data */

    ,CAST(SUM(CPUUtil) / NumNodes / CPUs / RSSInterval AS FLOAT) (NAMED "AvgCPUBusy")
    ,CAST(SUM(SPMAPhysReadKB + SPMAPhysPreReadKB + SPMAPhysWriteKB) / 1024.0 / NumNodes / RSSInterval AS FLOAT) (NAMED "AvgMBSecNode")
    FROM dbc.dbcinfo info,
    (
    SELECT
    thedate (FORMAT 'yyyy-mm-dd')(NAMED "LogDate")
    ,CAST(thetime AS INT) / 1000 * 1000   (FORMAT '99:99:99') (NAMED "LogTime")
    ,{spmainterval} (NAMED "SPMAInterval")
    ,NodeID
    ,NCpus

        /* CPU */

    ,CAST(SUM(CPUUExec+CPUUServ) AS FLOAT) (NAMED "CPUUtil")

        /* Physical I/O */

    ,CAST(SUM(FileAcqReadKB) AS FLOAT)  (NAMED "SPMAPhysReadKB")
    ,CAST(SUM(FilePreReadKB) AS FLOAT) (NAMED "SPMAPhysPreReadKB")
    ,CAST(SUM(FileWriteKB) AS FLOAT) (NAMED "SPMAPhysWriteKB")
    FROM pdcrinfo.ResUsageSpma
    WHERE ( ( THEDATE > {startdate} AND THETIME >= 0 ) OR
    ( THEDATE = {startdate} ) )
    AND
    ( ( THEDATE = {enddate} AND THETIME <= 240000 ) OR
    ( THEDATE < {enddate} ) )
    GROUP BY 1,2,3,4,5
    ) spma_dt
    WHERE  info.infokey (NOT CS) = 'VERSION' (NOT CS)
    GROUP BY 1,2,3,4,5,6,7,8
    ) core_dt
    ) tcore_metrics_eng
    GROUP BY 1,2,3,4,5,6) a
    ) b

;

CREATE VOLATILE TABLE CB_DAILY_TCORE_METRICS_RATIO ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      LOGDATE DATE FORMAT 'YYYY-MM-DD',
      CPU_RATIO DECIMAL(20,15),
      IO_RATIO DECIMAL(20,15),
      TCORE_HRS_DAY INTEGER,
      TCORERATIOPERDAY DECIMAL(20,10),
      JOB_NM VARCHAR(40) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      USR_NM VARCHAR(40) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      EDW_START_TSP TIMESTAMP(6) NOT NULL)
PRIMARY INDEX ( LOGDATE ) ON COMMIT PRESERVE ROWS

;

INSERT INTO  CB_DAILY_TCORE_METRICS_RATIO
SELECT
      logdate,
	  --hour_dt,
	  CPU_Ratio,
	  IO_Ratio,
	  TCore_hrs_day,
	  ratio as TCoreRatioPerDay,
		'CB_DAILY_TCORE_METRICS_Load',
		'CSM_Usr',
		current_timestamp(6)



FROM
(

SELECT c.logdate,
	   c.hour_dt,
	   cast((c.TCore_hrs_day*c.CPU_Ratio)/(b.cpuusage) as dec(20,15)) as CPU_Ratio,                       --Calc the adj CPU_Ratio without the Golden Gate CPU time
	   cast((c.TCore_hrs_day*c.IO_Ratio)/(b.TotalIOCount) as dec(20,15)) as IO_Ratio,                --Calc the adj IO_Ratio without the Gold Gate IO
	   c.TCore_hrs_day,
	   c."MTD_Avg",
	   c.TCore_hrs_MTD,
	   b.cpuusage,
	   cast(c.TCore_hrs_day/b.cpuusage as dec(20,10) ) as ratio
    FROM
		CB_DAILY_TCORE_METRICS c
	inner join

	(select logdate,
			EXTRACT(HOUR FROM collecttimestamp) AS HOUR_DT,
			cast((sum(l.AmpCPUTime + l.ParserCPUTime)) as dec(20,10)) as cpuusage,
		sum(TotalIOCount) as TotalIOCount
		from pdcrinfo.dbqlogtbl_hst  l
		where logdate >= {startdate}
		group by 1,2
    )b
	on c.logdate = b.logdate
	and  C.HOUR_DT = B.HOUR_DT

	) E


;

CREATE VOLATILE TABLE vt_cleansedSQL AS
(
	SELECT
			ql.procid,
			ql.queryid,
			qls.SQLRowNo,
		--		EDWDECODED.sqlnormalisation_WL(CAST (TRANSLATE(COALESCE(qls.sqltextinfo, ql.QueryText) USING UNICODE_TO_LATIN WITH ERROR) AS VARCHAR(25000))) cleansed
		(CAST (TRANSLATE(COALESCE(qls.sqltextinfo, ql.QueryText) USING UNICODE_TO_LATIN WITH ERROR) AS VARCHAR(25000))) cleansed
		FROM pdcrinfo.DbqLogTbl ql
		LEFT OUTER JOIN pdcrinfo.DBQLSqlTbl qls
			ON ql.QueryId = qls.QueryId
			AND ql.LogDate = qls.LogDate
			AND qls.LogDate > {startdate}
		WHERE ql.LogDate > {enddate}
		AND qls.SqlRowNo = 1
		AND TRANSLATE_CHK(qls.sqltextinfo USING UNICODE_TO_LATIN) = 0
		AND TRANSLATE_CHK(ql.QueryText USING UNICODE_TO_LATIN) = 0
		GROUP BY 1,2,3,4
) WITH DATA PRIMARY INDEX (queryid)
ON COMMIT PRESERVE ROWS

;

CREATE VOLATILE TABLE CB_ComplexityScore ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      LOGDATE DATE FORMAT 'YYYY-MM-DD',
      PROCID DECIMAL(5,0),
      QUERYID DECIMAL(18,0),
      QUERYCOMPLEXITYSCORE FLOAT,
      JOB_NM VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
      USR_NM VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
      EDW_START_TSP TIMESTAMP(6) NOT NULL)
PRIMARY INDEX ( LOGDATE ,PROCID ,QUERYID ) ON COMMIT PRESERVE ROWS

;


INSERT INTO CB_ComplexityScore

		SELECT
			q.procid as procid
		,	q.queryid (DECIMAL(18,0)) QueryId
		,	SUM(q.StepComplexityScore) (FLOAT) QueryComplexityScore
		FROM
		(

			SELECT
				dbql.procid
			,	dbql.queryid
			, 	sg.stepgroupname
			, 	sg.StepGroupDesc
			, 	SUM(COALESCE(sg.complexityweight,0)) StepComplexityScore
			--, count(*)  CountOfUses
			FROM
			(
				SELECT
					procId
				, 	queryId
				, 	stepName
				FROM  PDCRINFO.dbqlsteptbl
				WHERE logdate > {startdate}
			)  ST
			JOIN PDCRINFO.dbqlogtbl dbql
				ON dbql.procid  = ST.procid
				AND dbql.queryid = ST.queryid
			JOIN "consumption_step.csv" s
				ON s.StepName = ST.StepName
			JOIN "consumption_stepgrp.csv" sg
				ON sg.StepGroupName = s.StepGroup
			WHERE 1=1
			--AND sg.complexityweight IS NOT NULL
			AND dbql.LogDate > {startdate}
			GROUP BY 1,2,3,4

			UNION ALL

	 		/* M. Beek/April-2018: As we are now including the SQLRowNo from the QrylogSql table (to capture all SQL)
			** We will need to do a MAX aggregation here, not to double count the scores for long queries (as that is not done
			** for short queries either */
			SELECT
				dt.procid
			,	dt.queryid
			, 	dt.stepgroupname
			, 	dt.StepGroupDesc
			,	MAX(StepComplexityScore) AS StepComplexityScore
			FROM
			(
						SELECT
							SQ.procid
						,	SQ.queryid
						,	SQ.SQLRowNo
						, 	sg.stepgroupname
						, 	sg.StepGroupDesc
						, 	SUM(sg.complexityweight) StepComplexityScore
			                /* Match using patterns based upon common punctuation forms. */
			                /* In the examples, FN is the function name being matched. */
						FROM  vt_cleansedSQL SQ
						JOIN "stepgroupfunctions.csv" sgf
							ON 	SQ.cleansed like '% ' || sgf.functionname || ' %'   /* example: select FN ( */
							OR  SQ.cleansed like '% ' || sgf.functionname || '(%'   /* example: select FN( */
							OR  SQ.cleansed like '%,' || sgf.functionname || ' %'   /* example: select x,FN ( */
							OR  SQ.cleansed like '%,' || sgf.functionname || '(%'   /* example: select x, FN( */
							OR  SQ.cleansed like '%.' || sgf.functionname || ' %'   /* example: select TD_SYSFNLIB.FN ( */
							OR  SQ.cleansed like '%.' || sgf.functionname || '(%'   /* example: select TD_SYSFNLIB.FN( */
						JOIN "consumption_stepgrp.csv" sg
							ON sgf.stepgroupname = sg.stepgroupname
						GROUP BY 1,2,3,4,5
			) AS dt
			GROUP BY 1,2,3,4

		) q group by 1,2
	;


/*{{save:COA_DashboardDailyDetail.csv}}*/
SELECT        DISTINCT
                 l.QueryId,
                 l.LogDate AS LogDate,
                 EXTRACT(HOUR FROM l.collecttimestamp) AS LogHour,
                 l.UserName,
				 l.UserId,
				 'Unknown' as  FIRST_NAME          ,
				 'Unknown' as  LAST_NAME           ,
				 'Unknown' as  EMAIL_ADDR          ,
				 coalesce(d.REGION,'Unknown') as  REGION          ,
				 'Unknown' as  LOCATION          ,
				 coalesce(d.DEPARTMENT, 'Unknown') as  DEPARTMENT          ,
				 coalesce(d.SUBDEPARTMENT,'Unknown') as  SUBDEPARTMENT_L1       ,
				 'Unknown' as  SUBDEPARTMENT_L2       ,
				 'Unknown' as  SUBDEPARTMENT_L3       ,
				 'Unknown' as  DIRECT_MGR          ,
				 'Unknown' as  L5_MGR_NM          ,
				 'Unknown' as  L4_MGR_NM          ,
				 'Unknown' as  L3_MGR_NM          ,
				 'Unknown' as  L2_MGR_NM          ,
				 'Unknown' as  EMPLOYEE_STATUS ,
	             l.StatementType,
                 l.appid,
                 l.TotalIOCount,
                 l.Ampcputime,
                 cast(l.AmpCPUTime + l.ParserCPUTime as dec(15,7))          AS TotalCPU,
  				 r.TCoreRatioPerDay,
                 (cast((l.AmpCPUTime + l.ParserCPUTime) as Float) *  cast(r.CPU_Ratio as Float)) +  (cast(l.TotalIOCount as Float) *   cast(r.IO_Ratio as Float)) as TCoreUsage,
				 case when (COALESCE((c.QueryComplexityScore - e.minCompScore) / (e.maxCompScore - e.minCompScore),0)) < .2 THEN 0
					  WHEN (COALESCE((c.QueryComplexityScore - e.minCompScore) / (e.maxCompScore - e.minCompScore),0)) < .4 THEN 1
					  WHEN (COALESCE((c.QueryComplexityScore - e.minCompScore) / (e.maxCompScore - e.minCompScore),0)) < .6 THEN 2
					  WHEN (COALESCE((c.QueryComplexityScore - e.minCompScore) / (e.maxCompScore - e.minCompScore),0)) < .8 THEN 3
				ELSE 4
				END AS ComplexityLevel,

	            case
					when l.TotalIOCount = 0 then 0
					else (l.ampcputime*1000)/l.totaliocount
				end as PJI ,
				case
					when l.TotalIOCount = 0 or l.ampcputime = 0 then 0
					else l.totaliocount/(l.ampcputime*1000)
				end as UII ,
				'CB_DAILY_TCORE_METRICS_Load' as JOB_NM,
				User as USR_NM,
				Current_Timestamp(6) as EDW_START_TSP

                 0
    on l.logdate= r.logdate
	and  r.hour_dt = EXTRACT(HOUR FROM l.collecttimestamp)
   left outer join CB_ComplexityScore c
   	on    l.procid = c.procid
	and   l.queryid = c.queryid
	and   l.logdate = c.logdate


	left outer join ca_user_xref d

	on d.USERNAME = l.username

	left outer join (
	select
		min(queryComplexityScore) as minCompScore
		,max(queryComplexityScore) as maxCompScore
	from
	CB_ComplexityScore
	) e
	on 1= 1

	where logdate between {startdate} and {enddate}

;


CREATE MULTISET TABLE CB_DAILY_SUMMARY_CURR_PREV_WK ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      DateRange BYTEINT,
      MatchNm VARCHAR(25) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Date_Start DATE FORMAT 'yyyy-mm-dd',
      Date_End DATE FORMAT 'yyyy-mm-dd',
      DASH_DATE_LEVEL VARCHAR(25) CHARACTER SET UNICODE NOT CASESPECIFIC,
      DASH_DATE_LABEL VARCHAR(25) CHARACTER SET UNICODE NOT CASESPECIFIC,
      AGGREGATE_LEVEL VARCHAR(25) CHARACTER SET UNICODE NOT CASESPECIFIC,
      AGGREGATE_NAME VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      METRIC_NAME VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      DASH_METRIC_NAME VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      DASH_METRIC_NAME_SHORT VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      DASH_METRIC_DESC VARCHAR(250) CHARACTER SET UNICODE NOT CASESPECIFIC,
      MEASURE_AMT DECIMAL(38,10),
      BENCHMARK_AMT INTEGER,
      COMPLEXITY_SCORE INTEGER,
      NBR_ACTIVE_USERS INTEGER)
NO PRIMARY INDEX
;




insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'TC'
,{startdate} as Date_Start
,{enddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THUR'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from Sys_Calendar.CALENDAR where calendar_date = {enddate})
	  when ({daterange} = 3) then 'MONTH'
	  when ({daterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({daterange} = 1) then 'LW'
	  when ({daterange} = 3) then 'LM'
	  when ({daterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'SUMMARY' as AGGREGATE_LEVEL
,Null as AGGREGATE_NAME
,'TCORE HR TOTAL'  as METRIC_NAME
,'T-Core Hours Total' as DASH_METRIC_NAME
,'Total' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,sum(cast(TCoreUsage as dec(25,5))) as MEASURE_AMT
,Max(B.TCORE_BUDGET_HOURS) as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS

FROM	CB_DAILY_QUERY_METRICS A
inner join (

select 50000 as TCORE_BUDGET_HOURS
from dbc.dbcinfo
where START_DT >= {startdate} and END_DT <= (select monthend from Sys_Calendar.BusinessCalendar where calendar_date = {enddate})

 ) B
on 1=1


where A.LOGDATE between {startdate} and {enddate};




--Summary TCORE HR DAILY AVG
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'THDA'
,{startdate} as Date_Start
,{enddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (TD_Consumption_DB_BASE_T.day_of_week = 5) then 'THUR'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from Sys_Calendar.CALENDAR where calendar_date = {enddate})
	  when ({daterange} = 3) then 'MONTH'
	  when ({daterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({daterange} = 1) then 'LW'
	  when ({daterange} = 3) then 'LM'
	  when ({daterange} = 5) then 'LQ'
	"consumption_step.csv"  Else ''
	  END as DASH_DATE_LABEL
,'SUMMARY' as AGGREGATE_LEVEL
,Null as AGGREGATE_NAME
,'TCORE HR DAILY AVG'  as METRIC_NAME
,'T-Core Hours Daily Avg' as DASH_METRIC_NAME
,'Daily Avg' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,sum(cast(TCoreUsage as dec(25,5)))/(({enddate} - {startdate})+1) as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS

FROM	CB_DAILY_QUERY_METRICS A
where A.LOGDATE between {startdate} and {enddate};



--Summary Total TotalCpu
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'TTLCPU'
,{startdate} as Date_Start
,{enddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THUR'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from Sys_Calendar.CALENDAR where calendar_date = {enddate})
	  when ({daterange} = 3) then 'MONTH'
	  when ({daterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({daterange} = 1) then 'LW'
	  when ({daterange} = 3) then 'LM'
	  when ({daterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'SUMMARY' as AGGREGATE_LEVEL
,Null as AGGREGATE_NAME
,'CPU COMPUTE'  as METRIC_NAME
,'Compute CPU Seconds ' as DASH_METRIC_NAME
,'Compute' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,sum(cast(TotalCPU as dec(25,5))) as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS

FROM	CB_DAILY_QUERY_METRICS A
where A.LOGDATE between {startdate} and {enddate};

--Summary Total TotalIO
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'TTLIO'
,{startdate} as Date_Start
,{enddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THUR'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from Sys_Calendar.CALENDAR where calendar_date = {enddate})
	  when ({daterange} = 3) then 'MONTH'
	  when ({daterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({daterange} = 1) then 'LW'
	  when ({daterange} = 3) then 'LM'
	  when ({daterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'SUMMARY' as AGGREGATE_LEVEL
,Null as AGGREGATE_NAME
,'IO'  as METRIC_NAME
,'IO' as DASH_METRIC_NAME
,'IO' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,sum(cast(TotalIOCount as dec(25,5))) as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS

FROM	CB_DAILY_QUERY_METRICS A
where A.LOGDATE between {startdate} and {enddate};



--Summary NBR QUERIES
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'NBRQRY'
,{startdate} as Date_Start
,{enddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THUR'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from Sys_Calendar.CALENDAR where calendar_date = {enddate})
	  when ({daterange} = 3) then 'MONTH'
	  when ({daterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({daterange} = 1) then 'LW'
	  when ({daterange} = 3) then 'LM'
	  when ({daterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'SUMMARY' as AGGREGATE_LEVEL
,Null as AGGREGATE_NAME
,'NBR QUERIES'  as METRIC_NAME
,'NBR QUERIES' as DASH_METRIC_NAME
,'NBR QUERIES' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,cast(Count(Distinct Queryid ) as bigint) as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS

FROM	CB_DAILY_QUERY_METRICS A
where A.LOGDATE between {startdate} and {enddate};




--Summary ACTIVE USERS
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'ACTUSER'
,{startdate} as Date_Start
,{enddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THUR'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from Sys_Calendar.CALENDAR where calendar_date = {enddate})
	  when ({daterange} = 3) then 'MONTH'
	  when ({daterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({daterange} = 1) then 'LW'
	  when ({daterange} = 3) then 'LM'
	  when ({daterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'SUMMARY' as AGGREGATE_LEVEL
,Null as AGGREGATE_NAME
,'ACTIVE USERS'  as METRIC_NAME
,'ACTIVE USERS' as DASH_METRIC_NAME
,'ACTIVE USERS' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,count(distinct(userName)) as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS

FROM	CB_DAILY_QUERY_METRICS A
where A.LOGDATE between {startdate} and {enddate};

--Summary TCORE HR COST PER QUERY
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'TCHRCostPerQry'
,{startdate} as Date_Start
,{enddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THUR'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from Sys_Calendar.CALENDAR where calendar_date = {enddate})
	  when ({daterange} = 3) then 'MONTH'
	  when ({daterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({daterange} = 1) then 'LW'
	  when ({daterange} = 3) then 'LM'
	  when ({daterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'SUMMARY' as AGGREGATE_LEVEL
,Null as AGGREGATE_NAME
,'TCORE HR COST PER QUERY'  as METRIC_NAME
,'T-Core Hours Cost per Query' as DASH_METRIC_NAME
,'Per Query' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,(1000000)*1.000/cast(count(distinct queryid ) as dec(25,10))  as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS

FROM	CB_DAILY_QUERY_METRICS A
where A.LOGDATE between {startdate} and {enddate};

CREATE VOLATILE TABLE CB_DAILY_CONCURRENCY_WK ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      LOGDATE DATE FORMAT 'YYYY-MM-DD',
      AVGCON_AVG_PER_HR FLOAT,
      MAXCON_PEAK_PER_HR INTEGER,
      JOB_NM VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
      USR_NM VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
      EDW_START_TSP TIMESTAMP(6) NOT NULL)
PRIMARY INDEX ( LOGDATE )

;



--Summary vt_Concurrency
insert into CB_DAILY_CONCURRENCY_WK
select
logdate,
round(Avg(Concurrency_Avg)) as AvgCon_Avg_Per_hr,
Max(Concurrency_Peak) as MaxCon_Peak_Per_hr
,'CB_DAILY_CONCURRENCY_WK_LOAD'
,user
,current_timestamp

from
(
SELECT
 cast(StartTmHr as date) LogDate
,extract(HOUR from StartTmHr) as LogHour
,round(avg(PointConcurrency),0) as Concurrency_Avg
,max(PointConcurrency) as Concurrency_Peak
FROM
  (SELECT
   cast(SUBSTR(CAST(ClockTick AS  VARCHAR(30)), 1, 14) || '00:00' as timestamp(0))  StartTmHr
   , clockTick  /* Every 10 seconds */
   , SUM(QryCount)  PointConcurrency
     FROM
        (  /* the expand  by anchor second clause duplicates the dbql columns for each second between the firststeptime and firstresptime.
            grouping on the second tells us how many concurrent queries were running during that second */
        SELECT   BEGIN(Qper)  ClockTick
        ,cast(SUBSTR(CAST(ClockTick AS  VARCHAR(30)), 1, 17) || '0'  as timestamp(0)) as StartTm10s
        , CAST(1 AS SMALLINT) QryCount
        , PERIOD(firststeptime,firstresptime+ interval '1' second) QryDurationPeriod
        FROM pdcrinfo.dbqlogtbl as lg
        WHERE logdate   BETWEEN  {startdate}  AND {enddate}
          AND NumOfActiveAmps >  0
         EXPAND ON QryDurationPeriod AS Qper BY ANCHOR ANCHOR_SECOND
        ) qrylog
    WHERE  extract(second  from ClockTick) in (0,10,20,30,40,50)
    GROUP BY 1, 2
  ) ex
GROUP BY 1,2 ) a
GROUP BY 1

;



--Summary PEAK CONCURRENCY
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'PKCONC'
,{startdate} as Date_Start
,{enddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THUR'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from Sys_Calendar.CALENDAR where calendar_date = {enddate})
	  when ({daterange} = 3) then 'MONTH'
	  when ({daterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({daterange} = 1) then 'LW'
	  when ({daterange} = 3) then 'LM'
	  when ({daterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'SUMMARY' as AGGREGATE_LEVEL
,Null as AGGREGATE_NAME
,'PEAK CONCURRENCY'  as METRIC_NAME
,'PEAK CONCURRENCY' as DASH_METRIC_NAME
,'PEAK CONCURRENCY' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,MAX(MaxCon_Peak_Per_hr)  as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS

FROM	CB_DAILY_CONCURRENCY_WK  A
where A.LOGDATE between {startdate} and {enddate};



--Summary CONCURRENT USERS
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'CONCUSER'
,{startdate} as Date_Start
,{enddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THUR'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from Sys_Calendar.CALENDAR where calendar_date = {enddate})
	  when ({daterange} = 3) then 'MONTH'
	  when ({daterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({daterange} = 1) then 'LW'
	  when ({daterange} = 3) then 'LM'
	  when ({daterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'SUMMARY' as AGGREGATE_LEVEL
,Null as AGGREGATE_NAME
,'CONCURRENT USERS'  as METRIC_NAME
,'CONCURRENT USERS' as DASH_METRIC_NAME
,'CONCURRENT USERS' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,AVG(AvgCon_Avg_Per_hr)  as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS

FROM CB_DAILY_CONCURRENCY_WK A
where A.LOGDATE between {startdate} and {enddate};


CREATE VOLATILE TABLE CB_DAILY_EXTRACT_WK ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      LOGDATE DATE FORMAT 'YYYY-MM-DD',
      LOGICAL_IO_RATIO FLOAT,
      DATA_EGRESS FLOAT,
      TCORERATIOPERDAY DECIMAL(25,15),
      EXTRACTBYTCORE FLOAT,
      QUERYCPU FLOAT,
      POSSIBLE_PCT_OF_CPU_USED_FOR_EXTRACTS FLOAT,
      JOB_NM VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
      USR_NM VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
      EDW_START_TSP TIMESTAMP(6) NOT NULL)
PRIMARY INDEX ( LOGDATE )

;



insert into CB_DAILY_EXTRACT_WK
SELECT  t1.LOGDATE
		,SUM(LogIORat) as "Logical IO Ratio" -- percentage of io that is an extract
		,SUM(PossExCPU) as DATA_EGRESS
		,cast(avg(TCoreRatioPerDay)as dec(25,15)) as TCoreRatioPerDay
		,round(SUM(PossExCPU) * avg(TCoreRatioPerDay)) as ExtractByTCore
		,SUM(QueryCPU) as QueryCPU
		,SUM(PossExCPU)*100.00/SUM(QueryCPU) as "Possible Pct of CPU Used for Extracts" -- percentage of cpu dedicated to extracts
		,'CB_DAILY_EXTRACT_WK_LOAD'
		,user
		,current_timestamp
	--
FROM (	SELECT LogDate
		    , LogIORat
		    , PossExCPU
			, QueryCPU
		FROM ( 	select logdate
		  			,sum(case when UII > 1e1 then TotalIOCount else 0e0 end) PossLogIO -- intermediate
		  			,sum(TotalIOCOunt) QueryLogIO -- intermediate
		    		,PossLogIO / QueryLogIO as LogIORat  -- "Extract Index"
					-- "Pct of Total CPU used by extracts" is the next column (PossExCPU) divided by the sum of the column QueryCPU
		  			,sum(case when UII > 1e1 then AMPCPUTime else 0e0 end) as PossExCPU
		  			,sum(AMPCPUTime) as QueryCPU
					-- "Pct of total IO used by extracts" is PosLogIO divided by the sum of column QueryLogIO
				from (	Select logdate, AMPCPUTime ,TotalIOCount, ReqPhysIO, NumResultRows
		    				,Case when AMPCPUTime < 1e-1  or StatementType in all('Insert','Update','Delete','End loading','create table','checkpoint loading','help','collect statistics')
		                          or NumSteps = 0 or NumOfActiveAMPS = 0 then 0e0
		            		else TotalIOCount/(AMPCPUTime *1e3) end  as UII
		  				From  pdcrinfo.DBQLogTbl_hst
						where  logdate BETWEEN {startdate} and {enddate}           /* Modify window as desired */
						-- Since we're looking for high UII< there has to be SOME AMP work involved
		  				and NumSteps > 0 and AMPCPUTime > 0e0 and NumOfActiveAMPS > 0
		  				-- Elminate BAR
		  				and AppID <> 'DSMAIN' and UserName <> 'MOSDECODE02'
					) x1     /* Modify username pattern to exclude certain user ids */
		  			Group by 1
			) extracts
	) t1

	INNER JOIN CB_DAILY_TCORE_METRICS_RATIO t2
	ON t1.logdate = t2.logdate

	GROUP BY 1
;



--Summary EXTRACTS
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'EXT'
,{startdate} as Date_Start
,{enddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THUR'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from Sys_Calendar.CALENDAR where calendar_date = {enddate})
	  when ({daterange} = 3) then 'MONTH'
	  when ({daterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({daterange} = 1) then 'LW'
	  when ({daterange} = 3) then 'LM'
	  when ({daterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'SUMMARY' as AGGREGATE_LEVEL
,Null as AGGREGATE_NAME
,'EXTRACTS'  as METRIC_NAME
,'Extracts' as DASH_METRIC_NAME
,'Extracts' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,SUM(ExtractByTCore)  as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS

FROM	CB_DAILY_EXTRACT_WK  A
where A.LOGDATE between {startdate} and {enddate};



--Summary DATA EGRESS
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'EGRESS'
,{startdate} as Date_Start
,{enddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THUR'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from Sys_Calendar.CALENDAR where calendar_date = {enddate})
	  when ({daterange} = 3) then 'MONTH'
	  when ({daterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({daterange} = 1) then 'LW'
	  when ({daterange} = 3) then 'LM'
	  when ({daterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'SUMMARY' as AGGREGATE_LEVEL
,Null as AGGREGATE_NAME
,'DATA EGRESS'  as METRIC_NAME
,'Data Egress' as DASH_METRIC_NAME
,'Data Egress' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,SUM(DATA_EGRESS)/1099511627776  as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS

FROM	CB_DAILY_EXTRACT_WK  A
where A.LOGDATE between {startdate} and {enddate};


--Summary DATA INGRESS
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'INGRESS'
,{startdate} as Date_Start
,{enddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THUR'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from Sys_Calendar.CALENDAR where calendar_date = {enddate})
	  when ({daterange} = 3) then 'MONTH'
	  when ({daterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({daterange} = 1) then 'LW'
	  when ({daterange} = 3) then 'LM'
	  when ({daterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'SUMMARY' as AGGREGATE_LEVEL
,Null as AGGREGATE_NAME
,'DATA INGRESS'  as METRIC_NAME
,'Data Ingress' as DASH_METRIC_NAME
,'Data Ingress' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,SUM(currentperm)/1099511627776 as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS

FROM	pdcrinfo.TableSpace_Hst A
where A.LOGDATE between {startdate} and {enddate};



--Summary STORAGE
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'STORAGE'
,{startdate} as Date_Start
,{enddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THUR'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from Sys_Calendar.CALENDAR where calendar_date = {enddate})
	  when ({daterange} = 3) then 'MONTH'
	  when ({daterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({daterange} = 1) then 'LW'
	  when ({daterange} = 3) then 'LM'
	  when ({daterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'SUMMARY' as AGGREGATE_LEVEL
,Null as AGGREGATE_NAME
,'STORAGE'  as METRIC_NAME
,'Storage in TB' as DASH_METRIC_NAME
,'Storage' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
, Total_CDS_TB as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS

FROM	(


SELECT
       sum(Object_cds) as Total_CDS,
       CAST(ROUND(Total_CDS/1000,0) as INTEGER) as Total_CDS_TB
    FROM(
    SELECT CASE
        WHEN DBKIND = 'U' THEN 'User'
        WHEN DBKIND = 'D' THEN 'Database'
        ELSE 'Misc'
        END  Object_type,
        b.databasename as Object_name,
        1442833.9 AS CurrentPerm_Threshold,
        601180.8 AS system_CDS,
        CAST(SUM(a.CurrentPerm) AS FLOAT)/1000/1000/1000 AS Object_CurrentPerm_GB,
        CAST(((Object_CurrentPerm_GB/CurrentPerm_Threshold)*System_CDS) AS Float) AS Object_CDS
        FROM dbc.diskspace a
        JOIN dbc.databases b
        ON a.databasename=b.databasename
        GROUP BY 1,2,3,4) cds
 ) A
;



CREATE VOLATILE TABLE CB_DAILY_UTIL_WK ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      TTL BIGINT,
      ANSW BIGINT,
      MAIN BIGINT,
      ETL BIGINT,
      SYSPROC BIGINT,
      JOB_NM VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
      USR_NM VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
      EDW_START_TSP TIMESTAMP(6) NOT NULL)
NO PRIMARY INDEX ;


insert into CB_DAILY_UTIL_WK
select
(select cast(count(distinct queryid) as bigint) as ttl from
CB_DAILY_QUERY_METRICS
where logdate between {startdate} and {enddate}
) as ttl
,(select cast(count(distinct queryid) as bigint) as answ from
CB_DAILY_QUERY_METRICS
where statementtype in ('select')
and logdate between {startdate} and {enddate}) as Answ
,(select cast(count(distinct queryid) as bigint) as Main from
CB_DAILY_QUERY_METRICS
where statementtype  in ('collect statistics')
and logdate between{startdate} and {enddate}) as Main

,(select cast(count(distinct queryid) as bigint) as ETL from
CB_DAILY_QUERY_METRICS
where statementtype  in (
 'Merge Into'
,'Begin Loading'
,'Mload'
,'Delete'
,'End Loading'
,'Begin Delete Mload'
,'Update'
,'Exec'
,'Release Mload'
,'Insert'
,'Begin Mload'
,'Execute Mload'
,'Commit Work'
)
and logdate between{startdate} and {enddate}) as ETL


,cast(ttl - Answ - Main - ETL as bigint) as SysProc
,'CB_DAILY_UTIL_WK_LOAD'
,user
,current_timestamp

;


--UTILIZATION Answers
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'ANSW'
,{startdate} as Date_Start
,{enddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THUR'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from Sys_Calendar.CALENDAR where calendar_date = {enddate})
	  when ({daterange} = 3) then 'MONTH'
	  when ({daterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({daterange} = 1) then 'LW'
	  when ({daterange} = 3) then 'LM'
	  when ({daterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'UTILIZATION' as AGGREGATE_LEVEL
,Null as AGGREGATE_NAME
,'UTIL ANSWERS'  as METRIC_NAME
,'Answers' as DASH_METRIC_NAME
,'Answers' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,Answ as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM	CB_DAILY_UTIL_WK
;




--UTILIZATION UTIL INGEST ETL
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'ETL'
,{startdate} as Date_Start
,{enddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THUR'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from Sys_Calendar.CALENDAR where calendar_date = {enddate})
	  when ({daterange} = 3) then 'MONTH'
	  when ({daterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({daterange} = 1) then 'LW'
	  when ({daterange} = 3) then 'LM'
	  when ({daterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'UTILIZATION' as AGGREGATE_LEVEL
,NULL as AGGREGATE_NAME
,'UTIL INGEST ETL'  as METRIC_NAME
,'Ingest & ETL' as DASH_METRIC_NAME
,'Ingest & ETL' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,ETL as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM	CB_DAILY_UTIL_WK
;





--UTILIZATION UTIL MAINTENANCE
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'MAIN'
,{startdate} as Date_Start
,{enddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THUR'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from Sys_Calendar.CALENDAR where calendar_date = {enddate})
	  when ({daterange} = 3) then 'MONTH'
	  when ({daterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({daterange} = 1) then 'LW'
	  when ({daterange} = 3) then 'LM'
	  when ({daterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'UTILIZATION' as AGGREGATE_LEVEL
,NULL as AGGREGATE_NAME
,'UTIL MAINTENANCE'  as METRIC_NAME
,'Maintenance' as DASH_METRIC_NAME
,'Maintenance' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,Main as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM	CB_DAILY_UTIL_WK
;




--UTILIZATION UTIL System/Procedural
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'SysProc'
,{startdate} as Date_Start
,{enddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THUR'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from Sys_Calendar.CALENDAR where calendar_date = {enddate})
	  when ({daterange} = 3) then 'MONTH'
	  when ({daterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({daterange} = 1) then 'LW'
	  when ({daterange} = 3) then 'LM'
	  when ({daterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'UTILIZATION' as AGGREGATE_LEVEL
,NULL as AGGREGATE_NAME
,'UTIL System/Procedural'  as METRIC_NAME
,'System/Procedural' as DASH_METRIC_NAME
,'System/Procedural' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,SysProc as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM	CB_DAILY_UTIL_WK
;



CREATE VOLATILE TABLE CB_DAILY_DEPT_WK ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      DEPARTMENT VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
      ACTIVEUSER INTEGER,
      TCOREUSAGE DECIMAL(38,5),
      COMPLEXITYLEVEL INTEGER,
      COMPLEXITYSCOREPERUSER DECIMAL(38,5),
      JOB_NM VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
      USR_NM VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
      EDW_START_TSP TIMESTAMP(6) NOT NULL)
NO PRIMARY INDEX ;




Insert into CB_DAILY_DEPT_WK
Select
DEPARTMENT,
ActiveUser,
TCoreUsage,
ComplexityLevel,
ComplexityLevel/ActiveUser,
'CB_DAILY_DEPT_WK_LOAD',
USER,
current_timestamp
from
(
select DEPARTMENT,
count(distinct Username) as ActiveUser,
sum(ComplexityLevel) as ComplexityLevel,
sum(cast(TCoreUsage as dec(25,5))) as TCoreUsage
FROM	CB_DAILY_QUERY_METRICS
where LOGDATE between {startdate} and {enddate}
group by 1
) a
;







--DEPT RESOURCE SHARING  (TCORE HR TOTAL)
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,b.DEPARTMENT
,{startdate} as Date_Start
,{enddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THUR'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from Sys_Calendar.CALENDAR where calendar_date = {enddate})
	  when ({daterange} = 3) then 'MONTH'
	  when ({daterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({daterange} = 1) then 'LW'
	  when ({daterange} = 3) then 'LM'
	  when ({daterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'DEPT' as AGGREGATE_LEVEL
,b.DEPARTMENT as AGGREGATE_NAME
,'TCORE HR TOTAL'  as METRIC_NAME
,'TCORE HR TOTAL' as DASH_METRIC_NAME
,'TCORE HR TOTAL' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,b.TCoreUsage as MEASURE_AMT
,0 as BENCHMARK_AMT
,NULL as COMPLEXITY_SCORE
,Null as NBR_ACTIVE_USERS
FROM (


	select
	DEPARTMENT,
	TCoreUsage
	from
	(
			Select
			DEPARTMENT,
			sum(cast(TCoreUsage as dec(15,2))) as TCoreUsage
			from CB_DAILY_QUERY_METRICS A
			where a.logdate between {startdate} and {enddate}
			group by 1

	) a

) b;



--DEPT RESOURCE SHARING  (ACTIVE USERS)
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,b.DEPARTMENT
,{startdate} as Date_Start
,{enddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THUR'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from Sys_Calendar.CALENDAR where calendar_date = {enddate})
	  when ({daterange} = 3) then 'MONTH'
	  when ({daterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({daterange} = 1) then 'LW'
	  when ({daterange} = 3) then 'LM'
	  when ({daterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'DEPT' as AGGREGATE_LEVEL
,b.DEPARTMENT as AGGREGATE_NAME
,'ACTIVE USERS'  as METRIC_NAME
,'ACTIVE USERS' as DASH_METRIC_NAME
,'ACTIVE USERS' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,b.ActiveUser as MEASURE_AMT
,0 as BENCHMARK_AMT
,NULL as COMPLEXITY_SCORE
,NULL NBR_ACTIVE_USERS
FROM (


	select
	DEPARTMENT,
	ActiveUser
	from
	(
			Select
			DEPARTMENT,
			count(distinct userid) as ActiveUser
			from CB_DAILY_QUERY_METRICS A
			where A.LOGDATE between {startdate} and {enddate}
			group by 1

	) a
) b;


--DEPT RESOURCE SHARING  (NBR QUERIES)
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,b.DEPARTMENT
,{startdate} as Date_Start
,{enddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THUR'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from Sys_Calendar.CALENDAR where calendar_date = {enddate})
	  when ({daterange} = 3) then 'MONTH'
	  when ({daterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({daterange} = 1) then 'LW'
	  when ({daterange} = 3) then 'LM'
	  when ({daterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'DEPT' as AGGREGATE_LEVEL
,b.DEPARTMENT as AGGREGATE_NAME
,'NBR QUERIES'  as METRIC_NAME
,'NBR QUERIES' as DASH_METRIC_NAME
,'NBR QUERIES' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,cast(b.QueryNum  as bigint)as MEASURE_AMT
,0 as BENCHMARK_AMT
,NULL as COMPLEXITY_SCORE
,NULL NBR_ACTIVE_USERS
FROM (


	select
	DEPARTMENT,
	QueryNum
	from
	(
			Select
			DEPARTMENT,
			count(distinct queryid) as QueryNum
			from CB_DAILY_QUERY_METRICS A
			where A.LOGDATE between {startdate} and {enddate}
			group by 1

	) a
) b;


--DEPT RESOURCE SHARING  (PEAK CONCURRENCY and AvgConcurry Worktable)

CREATE VOLATILE TABLE CB_DAILY_CONCURRENCY_DEPT_WK ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      DEPARTMENT VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      CONCURRENCY_PEAK INTEGER,
      CONCURRENCY_AVG FLOAT,
      JOB_NM VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
      USR_NM VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
      EDW_START_TSP TIMESTAMP(6) NOT NULL)
PRIMARY INDEX ( DEPARTMENT );



insert into CB_DAILY_CONCURRENCY_DEPT_WK
select
DEPARTMENT,
Max(PointConcurrency) as Concurrency_Peak,
round(avg(PointConcurrency),0) as Concurrency_Avg,
'CB_DAILY_SUMMARY_CURR_PREV_LOAD',
user,
current_timestamp

from
(
  SELECT

   DEPARTMENT,
   cast(SUBSTR(CAST(ClockTick AS  VARCHAR(30)), 1, 14) || '00:00' as timestamp(0))  StartTmHr
   , clockTick  /* Every 10 seconds */
   , SUM(QryCount)  PointConcurrency
   ,(row_number() OVER(PARTITION BY StartTmHr ORDER BY PointConcurrency)- 1) * 100
                 / COUNT(*) OVER(PARTITION BY StartTmHr) AS Ntile   --Ntile for the 600 10 second samples within the hour
    FROM
        (  /* the expand  by anchor second clause duplicates the dbql columns for each second between the firststeptime and firstresptime.
            grouping on the second tells us how many concurrent queries were running during that second */
     SELECT
            BEGIN(Qper)  ClockTick
            ,cast(SUBSTR(CAST(ClockTick AS  VARCHAR(30)), 1, 17) || '0'  as timestamp(0)) as StartTm10s
            ,CAST(1 AS SMALLINT) QryCount
            ,PERIOD(firststeptime,firstresptime+ interval '1' second) QryDurationPeriod

            ,coalesce(QM.DEPARTMENT,'Unknown') as DEPARTMENT

        FROM
            pdcrinfo.dbqlogtbl as lg

			left outer join CA_USER_XREF QM

		on LG.USERNAME = QM.username




        WHERE
            LG.logdate  between {startdate} and {enddate}      --- CHANGE DATE RANGE WITH MACRO PARAMETERS
          AND NumOfActiveAmps >  0
          --AND QM.DEPARTMENT = 'ACCOUNTING'


         EXPAND ON QryDurationPeriod AS Qper BY ANCHOR ANCHOR_SECOND

        ) qrylog
    WHERE  extract(second  from ClockTick) in (0,10,20,30,40,50)  /* GIVES 600 POINTS PER 1 HOUR INTERVAL SO NTILE DOESN'T HAVE BIG EDGE EFFECT  */
    GROUP BY 1, 2,3
) a
group by 1
;

----DEPT RESOURCE SHARING  (PEAK CONCURRENCY)
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,b.DEPARTMENT
,{startdate} as Date_Start
,{enddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THUR'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from Sys_Calendar.CALENDAR where calendar_date = {enddate})
	  when ({daterange} = 3) then 'MONTH'
	  when ({daterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({daterange} = 1) then 'LW'
	  when ({daterange} = 3) then 'LM'
	  when ({daterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'DEPT' as AGGREGATE_LEVEL
,b.DEPARTMENT as AGGREGATE_NAME
,'PEAK CONCURRENCY'  as METRIC_NAME
,'PEAK CONCURRENCY' as DASH_METRIC_NAME
,'PEAK CONCURRENCY' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,b.Concurrency_Peak as MEASURE_AMT
,0 as BENCHMARK_AMT
,NULL as COMPLEXITY_SCORE
,NULL NBR_ACTIVE_USERS
FROM CB_DAILY_CONCURRENCY_DEPT_WK b;

----DEPT RESOURCE SHARING  (CONCURRENT USERS)
insert into  CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,b.DEPARTMENT
,{startdate} as Date_Start
,{enddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THUR'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from Sys_Calendar.CALENDAR where calendar_date = {enddate})
	  when ({daterange} = 3) then 'MONTH'
	  when ({daterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({daterange} = 1) then 'LW'
	  when ({daterange} = 3) then 'LM'
	  when ({daterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'DEPT' as AGGREGATE_LEVEL
,b.DEPARTMENT as AGGREGATE_NAME
,'CONCURRENT USERS'  as METRIC_NAME
,'CONCURRENT USERS' as DASH_METRIC_NAME
,'CONCURRENT USERS' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,b.Concurrency_Avg as MEASURE_AMT
,0 as BENCHMARK_AMT
,NULL as COMPLEXITY_SCORE
,NULL NBR_ACTIVE_USERS
FROM CB_DAILY_CONCURRENCY_DEPT_WK b;




--DEPT RESOURCE SHARING  (COMPLEXITY SCORE)
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,b.DEPARTMENT
,{startdate} as Date_Start
,{enddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THUR'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from Sys_Calendar.CALENDAR where calendar_date = {enddate})
	  when ({daterange} = 3) then 'MONTH'
	  when ({daterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({daterange} = 1) then 'LW'
	  when ({daterange} = 3) then 'LM'
	  when ({daterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'DEPT' as AGGREGATE_LEVEL
,b.DEPARTMENT as AGGREGATE_NAME
,'COMPLEXITY SCORE'  as METRIC_NAME
,'COMPLEXITY SCORE' as DASH_METRIC_NAME
,'COMPLEXITY SCORE' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,b.ComplexityLevel as MEASURE_AMT
,0 as BENCHMARK_AMT
,Null as COMPLEXITY_SCORE
,Null as NBR_ACTIVE_USERS
FROM (


	select
	DEPARTMENT,
	case when (ComplexityScore between .8 and 1 ) then 4
		when (ComplexityScore between .6 and .8 ) then 3
		when (ComplexityScore between .4 and .6 ) then 2
		when (ComplexityScore between .4 and .2 ) then 1
		else 0 end as ComplexityLevel
	from
	(
			Select
			DEPARTMENT,
			(ComplexityScorePerUser - minComplexityScorePerUser) / nullif ((maxComplexityScorePerUser - minComplexityScorePerUser),0) as ComplexityScore

			from CB_DAILY_DEPT_WK A
			inner join (select max(ComplexityScorePerUser) as maxComplexityScorePerUser,
							min(ComplexityScorePerUser) as minComplexityScorePerUser
			from .CB_DAILY_DEPT_WK) B



			on 1=1


	) a
) b;


);
