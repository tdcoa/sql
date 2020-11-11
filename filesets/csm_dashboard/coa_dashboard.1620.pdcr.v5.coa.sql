/* Start COA: COA_Dashboard
   see comments about each SQL step inline below.

Parameters:
  - siteid:       	{siteid}
  - ppstartdate:    {ppstartdate}
  - ppenddate:      {ppenddate}
  - cpstartdate:    {cpstartdate}
  - cpenddate:      {cpenddate}
  - dailystartdate: {dailystartdate}
  - dailyenddate:   {dailyenddate}
  - dbqlogtbl:    	{dbqlogtbl}
  - resusagespma: 	{resusagespma}
  - tcorebudget:  	{tcorebudget}
  - tcorecapacity:	{tcorecapacity}
  - qtroffset:		  {qtroffset}
  - systemcost:		  {systemcost}
  - dbqlflushrate:	{dbqlflushrate}
  - SPMAInterval:	  {spmainterval}
  - AvgMBSecRatio:	{avgmbsecratio}
  - daterange:		  {daterange}
*/



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
SELECT
			Logdate ,
			System_id,
			system_version,
			numnodes as System_Node_Cnt,
			System_TCore_Capacity,
			CPU_Ratio,
			IO_Ratio,
			CAST(ROUND(TCore_hrs_day,0) as INT) (named "TCore_hrs_day"),
			CAST(ROUND(TCore_hrs_MTD,0) as INT) (named "TCore_hrs_MTD"),
			CAST(ROUND(Daily_Avg,0) as INT) (named "MTD_Avg"),
			'CB_DAILY_TCORE_METRICS_Load' as JOB_NM,
			'CSM_Usr' as USR_NM,
			current_timestamp(6) as EDW_START_TSP
    FROM
    (
			SELECT
				System_id,
				system_version,
				numnodes,
				System_TCore_Capacity,
				Logdate,
				TCore_hrs_day,
				CPU_Ratio,
				IO_Ratio,
				TCore_hrs_MTD/EXTRACT(DAY
				FROM logdate) AS Daily_Avg,
				SUM(TCore_hrs_day) OVER (ORDER BY logdate ROWS UNBOUNDED PRECEDING) AS TCore_hrs_MTD
			FROM

			(
				SELECT
					System_id,
					system_version,
					System_TCore_Capacity,
					numnodes,
					Logdate,
					sum(TCore_CPU_cnt)/(sum(TCore_CPU_cnt) + sum(TCore_IO_cnt)) as CPU_Ratio,
					sum(TCore_IO_cnt)/(sum(TCore_CPU_cnt) + sum(TCore_IO_cnt)) as IO_Ratio,
					SUM(normalized_tcore_consumed)/{dbqlflushrate}  AS TCore_hrs_day
				FROM

					(
						SELECT
							CAST(core_dt.System_ID AS VARCHAR(100)) (NAMED "System_ID")
							,core_dt.NumNodes
							,core_dt.System_Version
							,core_dt.LogDate
							,core_dt.Timestamp_dt
							,core_dt.Hour_dt
							,CAST(core_dt.AvgCPUBusy/100 AS FLOAT) (NAMED "Percent_Compute_consumed")
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
										,EXTRACT(HOUR FROM "Timestamp_dt") (NAMED "Hour_dt")
										,SPMAInterval (NAMED "RSSInterval")

											/* System data */

										,'{siteid}' (NAMED "System_ID")
										,info.infodata (NAMED "System_Version")
										,{tcorecapacity} (NAMED "System_TCore_Capacity")
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
												WHERE ( ( THEDATE > ( {dailystartdate} ) AND THETIME >= 0      ) OR	( THEDATE = ( {dailystartdate} ) ) )
												AND   ( ( THEDATE = ( {dailyenddate}   ) AND THETIME <= 240000 ) OR ( THEDATE < ( {dailyenddate}   ) ) )
												GROUP BY 1,2,3,4,5
										) spma_dt
										WHERE  info.infokey (NOT CS) = 'VERSION' (NOT CS)
										GROUP BY 1,2,3,4,5,6,7,8
							) core_dt
					) tcore_metrics_eng
					GROUP BY 1,2,3,4,5
			) a
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

INSERT INTO CB_DAILY_TCORE_METRICS_RATIO
select
				logdate,
				CPU_Ratio,
				IO_Ratio,
				TCore_hrs_day,
				ratio as TCoreRatioPerDay,
				'CB_DAILY_TCORE_METRICS_Load' as JOB_NM,
				'CSM_User' as USR_NM,
				current_timestamp(6) as EDW_START_TSP



			from
			(

				SELECT
				c.logdate,
cast((c.TCore_hrs_day*c.CPU_Ratio)/(b.cpuusage /*+ d.GG_CPUTime*/) as dec(20,15)) as CPU_Ratio, /* o,                       Calc the adj CPU_Ratio without the Golden Gate CPU time */
cast((c.TCore_hrs_day*c.IO_Ratio)/(b.TotalIOCount /*+ d.GG_TotalIOCount*/) as dec(20,15)) as IO_Ratio, /* o,                Calc the adj IO_Ratio without the Gold Gate IO */
				c.TCore_hrs_day,
				c."MTD_Avg",
				c.TCore_hrs_MTD,
				b.cpuusage,
				cast(c.TCore_hrs_day/b.cpuusage as dec(20,10) ) as ratio
				FROM
CB_DAILY_TCORE_METRICS c /* S c                                                             Only the portion from DBQL Log */
				inner join
					(
						select
						logdate,
						cast((sum(l.AmpCPUTime + l.ParserCPUTime)) as dec(20,10)) as cpuusage,
						sum(TotalIOCount) as TotalIOCount
						from pdcrinfo.dbqlogtbl_hst  l
						where logdate between {dailystartdate} and {dailyenddate}
						group by 1
					)b
					on c.logdate = b.logdate
					/*left outer join                                                                                                   include Golden Gate from DBQLSummaryTbl_Hst table
					(
						SELECT	logdate,
								sum(TotalIOCount) as GG_TotalIOCount,
								sum( AMPCPUTime) as GG_CPUTime
						FROM	pdcrinfo.DBQLSummaryTbl_Hst
						where logdate between :DTE -(select COLUMNVALUE from CB_DIM_CONSTANT_REF where COLUMNNAME = 'DAY_BACK') and :DTE - 1
						and appid in ('GGSCI', 'Replicat')
						group by 1
					) d
					on c.logdate = d.logdate*/
			) E
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


CREATE MULTISET VOLATILE TABLE CB_DAILY_QUERY_METRICS ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      QUERYID DECIMAL(18,0),
      LOGDATE DATE FORMAT 'YYYY-MM-DD',
      LOGHOUR INTEGER,
      USERNAME VARCHAR(120) CHARACTER SET UNICODE NOT CASESPECIFIC,
      USERID BYTE(4),
      FIRST_NAME VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
      LAST_NAME VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
      EMAIL_ADDR VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC,
      REGION VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
	  LOCATION VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
      DEPARTMENT VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
      SUBDEPARTMENT_L1 VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
      SUBDEPARTMENT_L2 VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
      SUBDEPARTMENT_L3 VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
      DIRECT_MGR VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC ,
      L5_MGR_NM VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC ,
      L4_MGR_NM VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
      L3_MGR_NM VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
      L2_MGR_NM VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
      EMPLOYEE_STATUS VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
      STATEMENTTYPE CHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
      FINALWDNAME VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
      NUMSTEPS SMALLINT,
      APPID CHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
      TOTALIOCOUNT FLOAT,
      AMPCPUTIME FLOAT,
      TOTALCPU DECIMAL(15,7),
      TCORERATIOPERDAY DECIMAL(20,10),
      TCOREUSAGE DECIMAL(15,10),
      COMPLEXITYLEVEL BYTEINT,
      PJI FLOAT,
      UII FLOAT,
      JOB_NM VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC,
      USR_NM VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
      EDW_START_TSP TIMESTAMP(6) NOT NULL )
PRIMARY INDEX ( QUERYID )
PARTITION BY RANGE_N(LOGDATE  BETWEEN DATE '2020-01-01' AND '2025-12-31' EACH INTERVAL '1' DAY ) ON COMMIT PRESERVE ROWS
;

INSERT INTO CB_DAILY_QUERY_METRICS
SELECT        DISTINCT
                 l.QueryId,
                 l.LogDate AS LogDate,
                 EXTRACT(HOUR FROM l.collecttimestamp) AS LogHour,
                 d.UserHash as UserName,
				 l.UserId,
				 NULL as  FIRST_NAME          ,
				 NULL as  LAST_NAME           ,
				 NULL as  EMAIL_ADDR          ,
				 d.User_Region as  REGION          ,
				 NULL as  LOCATION          ,
				 d.User_Department as  DEPARTMENT          ,
				 d.User_SubDepartment as  SUBDEPARTMENT_L1       ,
				 NULL as  SUBDEPARTMENT_L2       ,
				 NULL as  SUBDEPARTMENT_L3       ,
				 NULL as  DIRECT_MGR          ,
				 NULL as  L5_MGR_NM          ,
				 NULL as  L4_MGR_NM          ,
				 NULL as  L3_MGR_NM          ,
				 NULL as  L2_MGR_NM          ,
				 NULL as  EMPLOYEE_STATUS ,
	             l.StatementType,
				 l.FINALWDNAME,
                 l.numsteps,
                 l.appid,
                 l.TotalIOCount,
                 l.Ampcputime,
                 cast(l.AmpCPUTime + l.ParserCPUTime as float)          AS TotalCPU,
  				 r.TCoreRatioPerDay,
                 (cast((l.AmpCPUTime + l.ParserCPUTime) as float) *  cast(r.CPU_Ratio as float)) +  (cast(l.TotalIOCount as float) *   cast(r.IO_Ratio as float)) as TCoreUsage,

				case
					when QueryComplexityScore between  0.01 and 10.00 then 1
					when QueryComplexityScore between 10.01 and 20.00 then 2
					when QueryComplexityScore between 20.01 and 30.00 then 3
					when QueryComplexityScore >= 30.01                then 4
					else 0
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
				'CSM_Usr' as USR_NM,
				Current_Timestamp(6) as EDW_START_TSP

   FROM
        pdcrinfo.dbqlogtbl_hst  l

   left outer join  CB_DAILY_TCORE_METRICS_RATIO r
    on l.logdate= r.logdate
   left outer join CB_ComplexityScore c
   	on    l.procid = c.procid
	and   l.queryid = c.queryid
	and   l.logdate = c.logdate


	left outer join dim_user d

	on d.USERNAME = l.username

	left outer join (
	select
		min(queryComplexityScore) as minCompScore
		,max(queryComplexityScore) as maxCompScore
	from
	CB_ComplexityScore
	) e
	on 1= 1
	where l.logdate between {dailystartdate} and {dailystartdate}



 /* - END OF DAILY DETAIL GRAIN */
;

/* START OF DAILY SUMMARY */
CREATE MULTISET VOLATILE TABLE CB_DAILY_SUMMARY_CURR_PREV_WK ,FALLBACK ,
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
NO PRIMARY INDEX ON COMMIT PRESERVE ROWS
;

insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'TC'
,{cpstartdate} as Date_Start
,{cpenddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {cpenddate})
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
inner join
 (
select {tcorebudget} as TCORE_BUDGET_HOURS
/*select sum(TCORE_BUDGET_HOURS) as TCORE_BUDGET_HOURS
from CB_DIM_BUDGET_REF
where START_DT >=
(case when 1 between 5 and 6 then (select  ADD_MONTHS(QuarterBegin, (SELECT cast(COLUMNVALUE as int) FROM  CB_DIM_CONSTANT_REF WHERE COLUMNNAME='QTR_OFFSET')) from CB_DIM_BusinessCalendar where calendar_date = {dailystartdate})
	  else {dailystartdate} end)
and END_DT <=
(case when 1 between 3 and 4 then (select monthend from CB_DIM_BusinessCalendar where calendar_date = {cpenddate})
	  when 1 between 5 and 6 then (select monthend from CB_DIM_BusinessCalendar where calendar_date in  (select ADD_MONTHS(QuarterEnd, (SELECT cast(COLUMNVALUE as int) FROM  CB_DIM_CONSTANT_REF WHERE COLUMNNAME='QTR_OFFSET')) from CB_DIM_BusinessCalendar where calendar_date = {cpenddate}))
	 else {cpenddate} end)*/
 ) B
	on 1=1
where A.LOGDATE between {cpstartdate} and {cpenddate}
;

/* Summary TCORE HR DAILY AVG */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'THDA'
,{cpstartdate} as Date_Start
,{cpenddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {cpenddate})
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
,'TCORE HR DAILY AVG'  as METRIC_NAME
,'T-Core Hours Daily Avg' as DASH_METRIC_NAME
,'Daily Avg' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,sum(cast(TCoreUsage as dec(25,5)))/(({cpenddate} - {cpstartdate})+1) as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM	CB_DAILY_QUERY_METRICS A
where A.LOGDATE between {cpstartdate} and {cpenddate}
;

/* Summary Total TotalCpu */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'TTLCPU'
,{cpstartdate} as Date_Start
,{cpenddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {cpenddate})
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
where A.LOGDATE between {cpstartdate} and {cpenddate}
;

/* Summary Total TotalIO */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'TTLIO'
,{cpstartdate} as Date_Start
,{cpenddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {cpenddate})
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
where A.LOGDATE between {cpstartdate} and {cpenddate}
;

/* Summary Nbr Queries */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'NBRQRY'
,{cpstartdate} as Date_Start
,{cpenddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {cpenddate})
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
,'Number of Queries' as DASH_METRIC_NAME
,'Queries' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,cast(Count(Distinct Queryid ) as bigint) as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM	CB_DAILY_QUERY_METRICS A
where A.LOGDATE between {cpstartdate} and {cpenddate}
and queryid > 0
;

/* Summary Nbr Queries GT 1 second */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'NBRQRYGT'
,{cpstartdate} as Date_Start
,{cpenddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {cpenddate})
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
,'NBR QUERIES GT1'  as METRIC_NAME
,'# Queries > 1 CPU Second' as DASH_METRIC_NAME
,'# Queries >1CPU' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,cast(Count(Distinct Queryid ) as bigint) as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM	CB_DAILY_QUERY_METRICS A
where A.LOGDATE between {cpstartdate} and {cpenddate}
and TotalCpu >= 1
and queryid > 0
;

/* Summary Active Users */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'ACTUSER'
,{cpstartdate} as Date_Start
,{cpenddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {cpenddate})
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
,'Active Users' as DASH_METRIC_NAME
,'Active Users' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,count(distinct(userName)) as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM	CB_DAILY_QUERY_METRICS A
where A.LOGDATE between {cpstartdate} and {cpenddate}
;

/* Summary TCORE HR COST PER QUERY */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'TCHRCostPerQry'
,{cpstartdate} as Date_Start
,{cpenddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {cpenddate})
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
,({systemcost}/12)/nullif(cast(count(distinct queryid ) as dec(25,10)),0)  as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM	CB_DAILY_QUERY_METRICS A
where A.LOGDATE between {cpstartdate} and {cpenddate}
and queryid > 0
;

CREATE MULTISET VOLATILE TABLE CB_DAILY_CONCURRENCY_WK ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      LOGDATE DATE FORMAT 'YYYY-MM-DD',
      LOGHOUR INTEGER,
      CONCURRENCY_AVG FLOAT,
      CONCURRENCY_PEAK INTEGER,
      JOB_NM VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
      USR_NM VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
      EDW_START_TSP TIMESTAMP(6) NOT NULL)
PRIMARY INDEX ( LOGDATE ) ON COMMIT PRESERVE ROWS
;

/* Summary vt_Concurrency */
insert into CB_DAILY_CONCURRENCY_WK
SELECT
 cast(StartTmHr as date) LogDate
,extract(HOUR from StartTmHr) as LogHour
,round(avg(PointConcurrency),0) as Concurrency_Avg
,max(PointConcurrency) as Concurrency_Peak
,'CB_DAILY_CONCURRENCY_WK_LOAD'
,user
,current_timestamp
FROM
  (SELECT
      cast(SUBSTR(CAST(ClockTick AS  VARCHAR(30)), 1, 14) || '00:00' as timestamp(0))  StartTmHr
   , clockTick  /* Every 10 seconds */
   , SUM(QryCount)  PointConcurrency
   ,(row_number() OVER(PARTITION BY StartTmHr ORDER BY PointConcurrency)- 1) * 100
/ COUNT(*) OVER(PARTITION BY StartTmHr) AS Ntile /* tTmHr) AS Ntile   Ntile for the 600 10 second samples within the hour */
    FROM
        (  /* the expand  by anchor second clause duplicates the dbql columns for each second between the firststeptime and firstresptime.
            grouping on the second tells us how many concurrent queries were running during that second */
        SELECT   BEGIN(Qper)  ClockTick
        ,cast(SUBSTR(CAST(ClockTick AS  VARCHAR(30)), 1, 17) || '0'  as timestamp(0)) as StartTm10s
        , CAST(1 AS SMALLINT) QryCount
         , case when   ( firststeptime< firstresptime+ interval '0.001' second)  then  PERIOD(firststeptime,firstresptime + interval '0.001' second) else PERIOD (firststeptime,firstresptime + interval '1' second) end QryDurationPeriod
        FROM pdcrinfo.dbqlogtbl as lg

        WHERE logdate   BETWEEN  {cpstartdate}  AND {cpenddate}
          AND NumOfActiveAmps >  0
          AND firststeptime <= firstresptime
         EXPAND ON QryDurationPeriod AS Qper BY ANCHOR ANCHOR_SECOND
        ) qrylog
    WHERE  extract(second  from ClockTick) in (0,10,20,30,40,50)  /* GIVES 600 POINTS PER 1 HOUR INTERVAL SO NTILE DOESNT HAVE BIG EDGE EFFECT  */
    GROUP BY 1, 2
  ) ex
GROUP BY 1,2
;

/* Summary Peak Concurrency */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'PKCONC'
,{cpstartdate} as Date_Start
,{cpenddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {cpenddate})
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
,'Peak Concurrency' as DASH_METRIC_NAME
,'Peak Concurrency' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,Max(Concurrency_Peak)  as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM	(
SELECT	logdate, sum(concurrency_avg) as concurrency_avg ,max(concurrency_peak) as concurrency_peak
FROM	CB_DAILY_CONCURRENCY_WK
group by 1)  A
;

/* Summary Concurrent Users */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'CONCUSER'
,{cpstartdate} as Date_Start
,{cpenddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {cpenddate})
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
,'Concurrent Users' as DASH_METRIC_NAME
,'Concurrent Users' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,round(avg(A.concurrency_avg))  as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM (
SELECT	logdate, sum(concurrency_avg) as concurrency_avg ,max(concurrency_peak) as concurrency_peak
FROM	CB_DAILY_CONCURRENCY_WK
group by 1)  A
;

CREATE MULTISET VOLATILE TABLE CB_DAILY_EXTRACT_WK ,FALLBACK ,
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
      EDW_START_TSP TIMESTAMP(6) NOT NULL
      )
PRIMARY INDEX ( LOGDATE ) ON COMMIT PRESERVE ROWS
;

insert into CB_DAILY_EXTRACT_WK
SELECT  t1.LOGDATE
,SUM(LogIORat) as "Logical IO Ratio" /*  percentage of io that is an extract */
		,SUM(PossExCPU) as DATA_EGRESS
		,cast(avg(TCoreRatioPerDay)as dec(25,15)) as TCoreRatioPerDay
		,round(SUM(PossExCPU) * avg(TCoreRatioPerDay)) as ExtractByTCore
		,SUM(QueryCPU) as QueryCPU
,SUM(PossExCPU)*100.00/SUM(QueryCPU) as "Possible Pct of CPU Used for Extracts" /*  percentage of cpu dedicated to extracts */
		,'CB_DAILY_EXTRACT_WK_LOAD'
		,user
		,current_timestamp
 /* - */
FROM (	SELECT LogDate
		    , LogIORat
		    , PossExCPU
			, QueryCPU
		FROM ( 	select logdate
,sum(case when UII > 1e1 then TotalIOCount else 0e0 end) PossLogIO /* LogIO  intermediate */
,sum(TotalIOCOunt) QueryLogIO /* LogIO  intermediate */
,PossLogIO / QueryLogIO as LogIORat /* gIORat   "Extract Index" */
 /*  "Pct of Total CPU used by extracts" is the next column (PossExCPU) divided by the sum of the column QueryCPU */
		  			,sum(case when UII > 1e1 then AMPCPUTime else 0e0 end) as PossExCPU
		  			,sum(AMPCPUTime) as QueryCPU
 /*  "Pct of total IO used by extracts" is PosLogIO divided by the sum of column QueryLogIO */
				from (	Select logdate, AMPCPUTime ,TotalIOCount, ReqPhysIO, NumResultRows
		    				,Case when AMPCPUTime < 1e-1  or StatementType in all('Insert','Update','Delete','End loading','create table','checkpoint loading','help','collect statistics')
		                          or NumSteps = 0 or NumOfActiveAMPS = 0 then 0e0
		            		else TotalIOCount/(AMPCPUTime *1e3) end  as UII
		  				From  pdcrinfo.DBQLogTbl_hst
						where  logdate BETWEEN {cpstartdate} and {cpenddate}           /* Modify window as desired */
 /*  Since were looking for high UII< there has to be SOME AMP work involved */
		  				and NumSteps > 0 and AMPCPUTime > 0e0 and NumOfActiveAMPS > 0
 /*  Elminate BAR */
		  				and AppID <> 'DSMAIN' and UserName <> 'MOSDECODE02'
					) x1     /* Modify username pattern to exclude certain user ids */
		  			Group by 1
			) extracts
	) t1

	INNER JOIN CB_DAILY_TCORE_METRICS_RATIO t2
	ON t1.logdate = t2.logdate

	GROUP BY 1
;

/*Update Data Egress*/

UPDATE CB_DAILY_EXTRACT_WK
FROM (SELECT thedate, (sum(HostReadKB)/1000/1000) as HostReadKB
	from PDCRINFO.resusagespma_hst where thedate between {cpstartdate} and {cpenddate} GROUP BY 1 ) B
SET data_egress = B.HostReadKB
WHERE logdate = b.thedate
;

/* Summary EXTRACTS */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'EXT'
,{cpstartdate} as Date_Start
,{cpenddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {cpenddate})
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
,'Extracts Daily Average' as DASH_METRIC_NAME
,'Extracts T-Core' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,AVG(ExtractByTCore)  as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM	CB_DAILY_EXTRACT_WK  A
where A.LOGDATE between {cpstartdate} and {cpenddate}
;

/* Summary DATA EGRESS */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'EGRESS'
,{cpstartdate} as Date_Start
,{cpenddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {cpenddate})
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
,'Data Egress Daily Average in GB' as DASH_METRIC_NAME
,'Data Egress (GB)' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,AVG(DATA_EGRESS)  as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM	CB_DAILY_EXTRACT_WK  A
where A.LOGDATE between {cpstartdate} and {cpenddate}
;

/* Summary DATA INGRESS */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'INGRESS'
,{cpstartdate} as Date_Start
,{cpenddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {cpenddate})
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
,'Data Ingress Daily Average in GB' as DASH_METRIC_NAME
,'Data Ingress (GB)' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,AVG(HostWriteKB)/1000/1000 as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM (SELECT sum(HostWriteKB) as HostWriteKB, thedate from PDCRINFO.resusagespma_hst where thedate between {cpstartdate} and {cpenddate} group by 2) A
/*pdcrinfo.TableSpace_Hst A where A.LOGDATE between {cpstartdate} and {cpenddate}*/
;

/* Summary STORAGE */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'STORAGE'
,{cpstartdate} as Date_Start
,{cpenddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {cpenddate})
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
,'Storage Daily Average in TB' as DASH_METRIC_NAME
,'Storage (TB)' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
, AVG(AVG_CDS_TB) as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM	(
SELECT logdate,
       SUM(Object_cds) as Total_CDS,
       CAST((Total_CDS/1000.00) as float) as AVG_CDS_TB
    FROM(
    SELECT
	    b.logdate,
        b.databasename as Object_name,
        {currentpermThreshold} AS CurrentPerm_Threshold,
        {systemcds}  AS system_CDS,
        CAST(SUM(a.CurrentPerm) AS FLOAT)/1000/1000/1000 AS Object_CurrentPerm_GB,
        CAST(((Object_CurrentPerm_GB/CurrentPerm_Threshold)*System_CDS) AS Float) AS Object_CDS
        FROM dbc.diskspace a
	    JOIN pdcrinfo.DatabaseSpace_Hst b
        ON a.databasename=b.databasename
		where LOGDATE between {cpstartdate} and {cpenddate}
        GROUP BY 1,2,3,4) cds

		group by 1

 ) A
;

/* UTIL */
CREATE MULTISET VOLATILE TABLE CB_DAILY_UTIL_WK ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      TTL DECIMAL(28,10),
      ANSW DECIMAL(28,10),
      MAIN DECIMAL(28,10),
      ETL DECIMAL(28,10),
      SYSPROC DECIMAL(28,10),
	  TTLWD DECIMAL(28,10),
	  ANALYTIC DECIMAL(28,10),
	  APPLICATION DECIMAL(28,10),
	  STREAM DECIMAL(28,10),
	  ETLWD DECIMAL(28,10),
	  SYSMGMT DECIMAL(28,10),
      JOB_NM VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
      USR_NM VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
      EDW_START_TSP TIMESTAMP(6) NOT NULL)
PRIMARY INDEX ( TTL ) ON COMMIT PRESERVE ROWS
;

insert into CB_DAILY_UTIL_WK
select
(select sum(cast(TCOREUSAGE as dec(15,5))) as ttl from
CB_DAILY_QUERY_METRICS
where logdate between {cpstartdate} and {cpenddate}
) as ttl
,(select sum(cast(TCOREUSAGE as dec(15,5))) as answ from
CB_DAILY_QUERY_METRICS a JOIN
dim_app b ON a.appid = b.appid
where use_bucket IN ('Analytic', 'Application')
and logdate between {cpstartdate} and {cpenddate}
and statementtype  not in ('collect statistics')
) as Answ
,(select sum(cast(TCOREUSAGE as dec(15,5))) as Main from
CB_DAILY_QUERY_METRICS
where statementtype  in ('collect statistics')
and logdate between {cpstartdate} and {cpenddate}
) as Main
,(select sum(cast(TCOREUSAGE as dec(15,5))) as ETL from
CB_DAILY_QUERY_METRICS a JOIN
dim_app b ON a.appid = b.appid
where use_bucket = 'ETL'
and logdate between {cpstartdate} and {cpenddate}
and statementtype  not in ('collect statistics')
) as ETL
,cast(ttl - Answ - Main - ETL as bigint) as SysProc
,(select ZEROIFNULL(sum(cast(TCOREUSAGE as dec(15,5)))) as TTLWD from
CB_DAILY_QUERY_METRICS
where logdate between {cpstartdate} and {cpenddate}
) as TTLWD
,(select ZEROIFNULL(sum(cast(TCOREUSAGE as dec(15,5)))) as ANALYTIC from
CB_DAILY_QUERY_METRICS a JOIN
dim_app b ON a.appid = b.appid
where use_bucket = 'Analytic'
and logdate between {cpstartdate} and {cpenddate}
) as ANALYTIC
,(select ZEROIFNULL(sum(cast(TCOREUSAGE as dec(15,5)))) as APPLICATION from
CB_DAILY_QUERY_METRICS a JOIN
dim_app b ON a.appid = b.appid
where use_bucket = 'Application'
and logdate between {cpstartdate} and {cpenddate}
) as APPLICATION
,(select ZEROIFNULL(sum(cast(TCOREUSAGE as dec(15,5)))) as STREAM from
CB_DAILY_QUERY_METRICS a JOIN
dim_app b ON a.appid = b.appid
where use_bucket = 'Stream'
and logdate between {cpstartdate} and {cpenddate}
) as STREAM
,(select ZEROIFNULL(sum(cast(TCOREUSAGE as dec(15,5)))) as ETLWD from
CB_DAILY_QUERY_METRICS a JOIN
dim_app b ON a.appid = b.appid
where use_bucket = 'ETL'
and logdate between {cpstartdate} and {cpenddate}
) as ETLWD
,cast(TTLWD - ANALYTIC - APPLICATION - STREAM - ETLWD as bigint) as SysMgmt
,'CB_DAILY_UTIL_WK_LOAD'
,user
,current_timestamp
;

/* UTILIZATION Answers */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'ANSW'
,{cpstartdate} as Date_Start
,{cpenddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {cpenddate})
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

/* UTILIZATION UTIL INGEST ETL */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'ETL'
,{cpstartdate} as Date_Start
,{cpenddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {cpenddate})
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

/* UTILIZATION UTIL MAINTENANCE */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'MAIN'
,{cpstartdate} as Date_Start
,{cpenddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {cpenddate})
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

/* UTILIZATION UTIL System/Procedural */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'SysProc'
,{cpstartdate} as Date_Start
,{cpenddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {cpenddate})
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
,'UTIL SYSTEM/PROCEDURAL'  as METRIC_NAME
,'System/Procedural' as DASH_METRIC_NAME
,'System/Procedural' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,SysProc as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM	CB_DAILY_UTIL_WK
;

/* UTILIZATION Workload AdHoc */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'Analytic'
,{cpstartdate} as Date_Start
,{cpenddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {cpenddate})
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
,'Analytic' as AGGREGATE_NAME
,'UTIL WORKLOAD TCORE'  as METRIC_NAME
,'Workload T-Core Usage ' as DASH_METRIC_NAME
,'Workload T-Core' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,Analytic as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM	CB_DAILY_UTIL_WK
;

/* UTILIZATION Workload BI */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'Application'
,{cpstartdate} as Date_Start
,{cpenddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {cpenddate})
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
,'Application' as AGGREGATE_NAME
,'UTIL WORKLOAD TCORE'  as METRIC_NAME
,'Workload T-Core Usage ' as DASH_METRIC_NAME
,'Workload T-Core' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,Application as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM	CB_DAILY_UTIL_WK
;

insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'Stream'
,{cpstartdate} as Date_Start
,{cpenddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {cpenddate})
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
,'Stream' as AGGREGATE_NAME
,'UTIL WORKLOAD TCORE'  as METRIC_NAME
,'Workload T-Core Usage ' as DASH_METRIC_NAME
,'Workload T-Core' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,Stream as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM	CB_DAILY_UTIL_WK
;

insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'ETLWD'
,{cpstartdate} as Date_Start
,{cpenddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {cpenddate})
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
,'ETL' as AGGREGATE_NAME
,'UTIL WORKLOAD TCORE'  as METRIC_NAME
,'Workload T-Core Usage ' as DASH_METRIC_NAME
,'Workload T-Core' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,ETLWD as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM	CB_DAILY_UTIL_WK
;


insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,'SysMgmt'
,{cpstartdate} as Date_Start
,{cpenddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {cpenddate})
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
,'Sys-Mgmt' as AGGREGATE_NAME
,'UTIL WORKLOAD TCORE'  as METRIC_NAME
,'Workload T-Core Usage ' as DASH_METRIC_NAME
,'Workload T-Core' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,SysMgmt as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM	CB_DAILY_UTIL_WK
;

/* DEPT */
CREATE MULTISET VOLATILE TABLE CB_DAILY_DEPT_WK ,FALLBACK ,
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
      EDW_START_TSP TIMESTAMP(6) NOT NULL
      )
NO PRIMARY INDEX ON COMMIT PRESERVE ROWS
;

Insert into CB_DAILY_DEPT_WK
Select
DEPARTMENT,
ActiveUser,
TCoreUsage,
ComplexityLevel,
ComplexityLevel/NULLIFZERO(ActiveUser),
'CB_DAILY_DEPT_WK_LOAD',
USER,
current_timestamp
from
(
select DEPARTMENT,
count(distinct Username) as ActiveUser,
sum(ComplexityLevel) as ComplexityLevel,
sum(cast(TCoreUsage as dec(38,10))) as TCoreUsage
FROM	CB_DAILY_QUERY_METRICS
where LOGDATE between {cpstartdate} and {cpenddate}
group by 1
) a
;

/* DEPT (Resource Sharing) */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,b.DEPARTMENT
,{cpstartdate} as Date_Start
,{cpenddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {cpenddate})
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
,'RESOURCE SHARING'  as METRIC_NAME
,'Resource Sharing' as DASH_METRIC_NAME
,'Resource Sharing' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,b.TCoreUsage / (select sum(TCoreUsage) from CB_DAILY_DEPT_WK)  as MEASURE_AMT
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
			sum(cast(TCoreUsage as dec(38,10))) as TCoreUsage
			from CB_DAILY_QUERY_METRICS A
			where a.logdate between {cpstartdate} and {cpenddate}
			group by 1

	) a




) b
;

/* DEPT (TCORE HR TOTAL) */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,b.DEPARTMENT
,{cpstartdate} as Date_Start
,{cpenddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {cpenddate})
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
,'T-Core Hours Total' as DASH_METRIC_NAME
,'Total' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,cast(b.TCoreUsage as dec(38,10)) as MEASURE_AMT
,B.TCORE_BUDGET_HOURS as BENCHMARK_AMT
,NULL as COMPLEXITY_SCORE
,Null as NBR_ACTIVE_USERS
FROM (
	select
	DEPARTMENT,
	TCoreUsage,
	{tcorebudget} as TCORE_BUDGET_HOURS
	from
	(
			Select
			DEPARTMENT,
			sum(cast(TCoreUsage as dec(38,10))) as TCoreUsage
			from CB_DAILY_QUERY_METRICS A
			where a.logdate between {cpstartdate} and {cpenddate}
			group by 1

	) a



	left outer join
 (
select 50000 as TCORE_BUDGET_HOURS
/*select sum(TCORE_BUDGET_HOURS) as TCORE_BUDGET_HOURS
from CB_DIM_BUDGET_REF
where START_DT >=
(
    case when 1 between 1 and 4 then (select  MonthBegin from CB_DIM_BusinessCalendar where calendar_date = {cpstartdate})
	     when 1 between 5 and 6 then (select  ADD_MONTHS(QuarterBegin, (SELECT cast(COLUMNVALUE as int) FROM  CB_DIM_CONSTANT_REF WHERE COLUMNNAME='QTR_OFFSET')) from CB_DIM_BusinessCalendar where calendar_date = {cpstartdate})
	else {cpstartdate} end)
and END_DT <=
(
	case  when 1 between 1 and 4 then (select monthend from CB_DIM_BusinessCalendar where calendar_date = {cpenddate})
		  when 1 between 5 and 6 then (select monthend from CB_DIM_BusinessCalendar where calendar_date in  (select ADD_MONTHS(QuarterEnd, (SELECT cast(COLUMNVALUE as int) FROM  CB_DIM_CONSTANT_REF WHERE COLUMNNAME='QTR_OFFSET')) from CB_DIM_BusinessCalendar where calendar_date = {cpenddate}))
	else {cpenddate} end)*/
 ) B
	on 1=1


) b
;

/* DEPT  (Active Users) */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,b.DEPARTMENT
,{cpstartdate} as Date_Start
,{cpenddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {cpenddate})
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
,'Active Users' as DASH_METRIC_NAME
,'Active Users' as DASH_METRIC_NAME_SHORT
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
			where A.LOGDATE between {cpstartdate} and {cpenddate}
			group by 1

	) a
) b
;

/* DEPT  (Nbr Queries) */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,b.DEPARTMENT
,{cpstartdate} as Date_Start
,{cpenddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {cpenddate})
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
,'Number of Queries' as DASH_METRIC_NAME
,'Queries' as DASH_METRIC_NAME_SHORT
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
			where A.LOGDATE between {cpstartdate} and {cpenddate}
			and queryid > 0
			group by 1

	) a
) b
;

/* DEPT  (NBR QUERIES GT1) */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,b.DEPARTMENT
,{cpstartdate} as Date_Start
,{cpenddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {cpenddate})
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
,'NBR QUERIES GT1'  as METRIC_NAME
,'# Queries > 1 CPU Second' as DASH_METRIC_NAME
,'# Queries >1CPU' as DASH_METRIC_NAME_SHORT
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
			where A.LOGDATE between {cpstartdate} and {cpenddate}
			and queryid > 0
			and TotalCpu >= 1
			group by 1

	) a
) b
;

/* DEPT  (Peak Concurrency and AvgConcurry Worktable) */
CREATE MULTISET VOLATILE TABLE CB_DAILY_CONCURRENCY_DEPT_WK AS
(
SELECT
DEPARTMENT
,cast(StartTmHr as date) LogDate
,extract(HOUR from StartTmHr) as LogHour
,round(avg(PointConcurrency),0) as Concurrency_Avg
,max(PointConcurrency) as Concurrency_Peak
,'CB_DAILY_SUMMARY_CURR_PREV_LOAD' AS JOB_NM
,user AS USR_NM
,current_timestamp AS EDW_START_TSP
FROM
  (SELECT
   User_DEPARTMENT as Department,
   cast(SUBSTR(CAST(ClockTick AS  VARCHAR(30)), 1, 14) || '00:00' as timestamp(0))  StartTmHr
   , clockTick  /* Every 10 seconds */
   , SUM(QryCount)  PointConcurrency
   ,(row_number() OVER(PARTITION BY StartTmHr ORDER BY PointConcurrency)- 1) * 100
/ COUNT(*) OVER(PARTITION BY StartTmHr) AS Ntile /* tTmHr) AS Ntile   Ntile for the 600 10 second samples within the hour */
    FROM
        (  /* the expand  by anchor second clause duplicates the dbql columns for each second between the firststeptime and firstresptime.
            grouping on the second tells us how many concurrent queries were running during that second */
        SELECT   BEGIN(Qper)  ClockTick
    ,User_DEPARTMENT
        ,cast(SUBSTR(CAST(ClockTick AS  VARCHAR(30)), 1, 17) || '0'  as timestamp(0)) as StartTm10s
        , CAST(1 AS SMALLINT) QryCount
        , PERIOD(firststeptime,firstresptime+ interval '0.001' second) QryDurationPeriod
        FROM pdcrinfo.dbqlogtbl as lg

      left outer join dim_user QM

    on LG.USERNAME = QM.username



        WHERE logdate   BETWEEN  {cpstartdate}  AND {cpenddate}
          AND NumOfActiveAmps >  0
         EXPAND ON QryDurationPeriod AS Qper BY ANCHOR ANCHOR_SECOND
        ) qrylog
    WHERE  extract(second  from ClockTick) in (0,10,20,30,40,50)  /* GIVES 600 POINTS PER 1 HOUR INTERVAL SO NTILE DOESNT HAVE BIG EDGE EFFECT  */
    GROUP BY 1, 2, 3
  ) ex
GROUP BY 1,2,3
) WITH DATA
PRIMARY INDEX(DEPARTMENT,LOGDATE) ON COMMIT PRESERVE ROWS
;

/* DEPT  (Peak Concurrency) */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,b.DEPARTMENT
,{cpstartdate} as Date_Start
,{cpenddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {cpenddate})
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
,'Peak Concurrency' as DASH_METRIC_NAME
,'Peak Concurrency' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,MAX(b.Concurrency_Peak) as MEASURE_AMT
,0 as BENCHMARK_AMT
,NULL as COMPLEXITY_SCORE
,NULL NBR_ACTIVE_USERS
FROM
(
SELECT	logdate, DEPARTMENT,max(concurrency_peak) as concurrency_peak
FROM	CB_DAILY_CONCURRENCY_DEPT_WK
group by 1,2
) b
GROUP BY DEPARTMENT
;

/* DEPT  (Concurrent Users) */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,b.DEPARTMENT
,{cpstartdate} as Date_Start
,{cpenddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {cpenddate})
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
,'Concurrent Users' as DASH_METRIC_NAME
,'Concurrent Users' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,round(avg(b.concurrency_avg))  as MEASURE_AMT
,0 as BENCHMARK_AMT
,NULL as COMPLEXITY_SCORE
,NULL NBR_ACTIVE_USERS
FROM (
SELECT	logdate, DEPARTMENT, sum(concurrency_avg) as concurrency_avg
FROM	CB_DAILY_CONCURRENCY_DEPT_WK
group by 1,2) b
GROUP BY DEPARTMENT
;

/* DEPT  (Complexity Score) */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {daterange} as DateRange
,b.DEPARTMENT
,{cpstartdate} as Date_Start
,{cpenddate} as Date_End
,case when ({daterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {cpenddate})
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
,'Complexity Score' as DASH_METRIC_NAME
,'Complexity' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,b.ComplexityLevel as MEASURE_AMT
,0 as BENCHMARK_AMT
,Null as COMPLEXITY_SCORE
,Null as NBR_ACTIVE_USERS
FROM (
	select
	DEPARTMENT,
	case when (ComplexityScore > .5 ) then 4
		when (ComplexityScore between .3 and .5 ) then 3
		when (ComplexityScore between .05 and .2 ) then 2
		when (ComplexityScore > 0 ) then 1
		else 0 end as ComplexityLevel
	from
	(
			Select
			DEPARTMENT,
			(ComplexityScorePerUser - minComplexityScorePerUser) / nullif ((maxComplexityScorePerUser - minComplexityScorePerUser),0) as ComplexityScore

			from CB_DAILY_DEPT_WK A
			inner join (select max(ComplexityScorePerUser) as maxComplexityScorePerUser,
							min(ComplexityScorePerUser) as minComplexityScorePerUser
			from CB_DAILY_DEPT_WK) B



			on 1=1


	) a
) b
;

/* START OF PREVIOUS PERIOD */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {prevdaterange} as DateRange
,'TC'
,{ppstartdate} as Date_Start
,{ppenddate} as Date_End
,case when ({prevdaterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {ppenddate})
	  when ({prevdaterange} = 3) then 'MONTH'
	  when ({prevdaterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({prevdaterange} = 1) then 'LW'
	  when ({prevdaterange} = 3) then 'LM'
	  when ({prevdaterange} = 5) then 'LQ'
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
inner join
 (
select {tcorebudget} as TCORE_BUDGET_HOURS
/*select sum(TCORE_BUDGET_HOURS) as TCORE_BUDGET_HOURS
from CB_DIM_BUDGET_REF
where START_DT >=
(case when 2 between 5 and 6 then (select  ADD_MONTHS(QuarterBegin, (SELECT cast(COLUMNVALUE as int) FROM  CB_DIM_CONSTANT_REF WHERE COLUMNNAME='QTR_OFFSET')) from CB_DIM_BusinessCalendar where calendar_date = {dailystartdate})
	  else {dailystartdate} end)
and END_DT <=
(case when 2 between 3 and 4 then (select monthend from CB_DIM_BusinessCalendar where calendar_date = {ppenddate})
	  when 2 between 5 and 6 then (select monthend from CB_DIM_BusinessCalendar where calendar_date in  (select ADD_MONTHS(QuarterEnd, (SELECT cast(COLUMNVALUE as int) FROM  CB_DIM_CONSTANT_REF WHERE COLUMNNAME='QTR_OFFSET')) from CB_DIM_BusinessCalendar where calendar_date = {ppenddate}))
	 else {ppenddate} end)*/
 ) B
	on 1=1
where A.LOGDATE between {ppstartdate} and {ppenddate}
;

/* Summary TCORE HR DAILY AVG */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {prevdaterange} as DateRange
,'THDA'
,{ppstartdate} as Date_Start
,{ppenddate} as Date_End
,case when ({prevdaterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {ppenddate})
	  when ({prevdaterange} = 3) then 'MONTH'
	  when ({prevdaterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({prevdaterange} = 1) then 'LW'
	  when ({prevdaterange} = 3) then 'LM'
	  when ({prevdaterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'SUMMARY' as AGGREGATE_LEVEL
,Null as AGGREGATE_NAME
,'TCORE HR DAILY AVG'  as METRIC_NAME
,'T-Core Hours Daily Avg' as DASH_METRIC_NAME
,'Daily Avg' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,sum(cast(TCoreUsage as dec(25,5)))/(({ppenddate} - {ppstartdate})+1) as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM	CB_DAILY_QUERY_METRICS A
where A.LOGDATE between {ppstartdate} and {ppenddate}
;

/* Summary Total TotalCpu */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {prevdaterange} as DateRange
,'TTLCPU'
,{ppstartdate} as Date_Start
,{ppenddate} as Date_End
,case when ({prevdaterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {ppenddate})
	  when ({prevdaterange} = 3) then 'MONTH'
	  when ({prevdaterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({prevdaterange} = 1) then 'LW'
	  when ({prevdaterange} = 3) then 'LM'
	  when ({prevdaterange} = 5) then 'LQ'
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
where A.LOGDATE between {ppstartdate} and {ppenddate}
;

/* Summary Total TotalIO */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {prevdaterange} as DateRange
,'TTLIO'
,{ppstartdate} as Date_Start
,{ppenddate} as Date_End
,case when ({prevdaterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {ppenddate})
	  when ({prevdaterange} = 3) then 'MONTH'
	  when ({prevdaterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({prevdaterange} = 1) then 'LW'
	  when ({prevdaterange} = 3) then 'LM'
	  when ({prevdaterange} = 5) then 'LQ'
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
where A.LOGDATE between {ppstartdate} and {ppenddate}
;

/* Summary Nbr Queries */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {prevdaterange} as DateRange
,'NBRQRY'
,{ppstartdate} as Date_Start
,{ppenddate} as Date_End
,case when ({prevdaterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {ppenddate})
	  when ({prevdaterange} = 3) then 'MONTH'
	  when ({prevdaterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({prevdaterange} = 1) then 'LW'
	  when ({prevdaterange} = 3) then 'LM'
	  when ({prevdaterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'SUMMARY' as AGGREGATE_LEVEL
,Null as AGGREGATE_NAME
,'NBR QUERIES'  as METRIC_NAME
,'Number of Queries' as DASH_METRIC_NAME
,'Queries' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,cast(Count(Distinct Queryid ) as bigint) as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM	CB_DAILY_QUERY_METRICS A
where A.LOGDATE between {ppstartdate} and {ppenddate}
and queryid > 0
;

/* Summary Nbr Queries GT 1 second */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {prevdaterange} as DateRange
,'NBRQRYGT'
,{ppstartdate} as Date_Start
,{ppenddate} as Date_End
,case when ({prevdaterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {ppenddate})
	  when ({prevdaterange} = 3) then 'MONTH'
	  when ({prevdaterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({prevdaterange} = 1) then 'LW'
	  when ({prevdaterange} = 3) then 'LM'
	  when ({prevdaterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'SUMMARY' as AGGREGATE_LEVEL
,Null as AGGREGATE_NAME
,'NBR QUERIES GT1'  as METRIC_NAME
,'# Queries > 1 CPU Second' as DASH_METRIC_NAME
,'# Queries >1CPU' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,cast(Count(Distinct Queryid ) as bigint) as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM	CB_DAILY_QUERY_METRICS A
where A.LOGDATE between {ppstartdate} and {ppenddate}
and TotalCpu >= 1
and queryid > 0
;

/* Summary Active Users */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {prevdaterange} as DateRange
,'ACTUSER'
,{ppstartdate} as Date_Start
,{ppenddate} as Date_End
,case when ({prevdaterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {ppenddate})
	  when ({prevdaterange} = 3) then 'MONTH'
	  when ({prevdaterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({prevdaterange} = 1) then 'LW'
	  when ({prevdaterange} = 3) then 'LM'
	  when ({prevdaterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'SUMMARY' as AGGREGATE_LEVEL
,Null as AGGREGATE_NAME
,'ACTIVE USERS'  as METRIC_NAME
,'Active Users' as DASH_METRIC_NAME
,'Active Users' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,count(distinct(userName)) as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM	CB_DAILY_QUERY_METRICS A
where A.LOGDATE between {ppstartdate} and {ppenddate}
;

/* Summary TCORE HR COST PER QUERY */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {prevdaterange} as DateRange
,'TCHRCostPerQry'
,{ppstartdate} as Date_Start
,{ppenddate} as Date_End
,case when ({prevdaterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {ppenddate})
	  when ({prevdaterange} = 3) then 'MONTH'
	  when ({prevdaterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({prevdaterange} = 1) then 'LW'
	  when ({prevdaterange} = 3) then 'LM'
	  when ({prevdaterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'SUMMARY' as AGGREGATE_LEVEL
,Null as AGGREGATE_NAME
,'TCORE HR COST PER QUERY'  as METRIC_NAME
,'T-Core Hours Cost per Query' as DASH_METRIC_NAME
,'Per Query' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,({systemcost}/12)/nullif(cast(count(distinct queryid ) as dec(25,10)),0)  as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM	CB_DAILY_QUERY_METRICS A
where A.LOGDATE between {ppstartdate} and {ppenddate}
and queryid > 0
;

/* Summary vt_Concurrency */
insert into CB_DAILY_CONCURRENCY_WK
SELECT
 cast(StartTmHr as date) LogDate
,extract(HOUR from StartTmHr) as LogHour
,round(avg(PointConcurrency),0) as Concurrency_Avg
,max(PointConcurrency) as Concurrency_Peak
,'CB_DAILY_CONCURRENCY_WK_LOAD'
,user
,current_timestamp
FROM
  (SELECT
      cast(SUBSTR(CAST(ClockTick AS  VARCHAR(30)), 1, 14) || '00:00' as timestamp(0))  StartTmHr
   , clockTick  /* Every 10 seconds */
   , SUM(QryCount)  PointConcurrency
   ,(row_number() OVER(PARTITION BY StartTmHr ORDER BY PointConcurrency)- 1) * 100
/ COUNT(*) OVER(PARTITION BY StartTmHr) AS Ntile /* tTmHr) AS Ntile   Ntile for the 600 10 second samples within the hour */
    FROM
        (  /* the expand  by anchor second clause duplicates the dbql columns for each second between the firststeptime and firstresptime.
            grouping on the second tells us how many concurrent queries were running during that second */
        SELECT   BEGIN(Qper)  ClockTick
        ,cast(SUBSTR(CAST(ClockTick AS  VARCHAR(30)), 1, 17) || '0'  as timestamp(0)) as StartTm10s
        , CAST(1 AS SMALLINT) QryCount
         , case when   ( firststeptime< firstresptime+ interval '0.001' second)  then  PERIOD(firststeptime,firstresptime + interval '0.001' second) else PERIOD (firststeptime,firstresptime + interval '1' second) end QryDurationPeriod
        FROM pdcrinfo.dbqlogtbl as lg

        WHERE logdate   BETWEEN  {ppstartdate}  AND {ppenddate}
          AND NumOfActiveAmps >  0
         EXPAND ON QryDurationPeriod AS Qper BY ANCHOR ANCHOR_SECOND
        ) qrylog
    WHERE  extract(second  from ClockTick) in (0,10,20,30,40,50)  /* GIVES 600 POINTS PER 1 HOUR INTERVAL SO NTILE DOESNT HAVE BIG EDGE EFFECT  */
    GROUP BY 1, 2
  ) ex
GROUP BY 1,2
;

/* Summary Peak Concurrency */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {prevdaterange} as DateRange
,'PKCONC'
,{ppstartdate} as Date_Start
,{ppenddate} as Date_End
,case when ({prevdaterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {ppenddate})
	  when ({prevdaterange} = 3) then 'MONTH'
	  when ({prevdaterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({prevdaterange} = 1) then 'LW'
	  when ({prevdaterange} = 3) then 'LM'
	  when ({prevdaterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'SUMMARY' as AGGREGATE_LEVEL
,Null as AGGREGATE_NAME
,'PEAK CONCURRENCY'  as METRIC_NAME
,'Peak Concurrency' as DASH_METRIC_NAME
,'Peak Concurrency' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,Max(Concurrency_Peak)  as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM	(
SELECT	logdate, sum(concurrency_avg) as concurrency_avg ,max(concurrency_peak) as concurrency_peak
FROM	CB_DAILY_CONCURRENCY_WK
group by 1)  A
;

/* Summary Concurrent Users */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {prevdaterange} as DateRange
,'CONCUSER'
,{ppstartdate} as Date_Start
,{ppenddate} as Date_End
,case when ({prevdaterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {ppenddate})
	  when ({prevdaterange} = 3) then 'MONTH'
	  when ({prevdaterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({prevdaterange} = 1) then 'LW'
	  when ({prevdaterange} = 3) then 'LM'
	  when ({prevdaterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'SUMMARY' as AGGREGATE_LEVEL
,Null as AGGREGATE_NAME
,'CONCURRENT USERS'  as METRIC_NAME
,'Concurrent Users' as DASH_METRIC_NAME
,'Concurrent Users' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,round(avg(A.concurrency_avg))  as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM (
SELECT	logdate, sum(concurrency_avg) as concurrency_avg ,max(concurrency_peak) as concurrency_peak
FROM	CB_DAILY_CONCURRENCY_WK
group by 1)  A
;

insert into CB_DAILY_EXTRACT_WK
SELECT  t1.LOGDATE
,SUM(LogIORat) as "Logical IO Ratio" /*  percentage of io that is an extract */
		,SUM(PossExCPU) as DATA_EGRESS
		,cast(avg(TCoreRatioPerDay)as dec(25,15)) as TCoreRatioPerDay
		,round(SUM(PossExCPU) * avg(TCoreRatioPerDay)) as ExtractByTCore
		,SUM(QueryCPU) as QueryCPU
,SUM(PossExCPU)*100.00/SUM(QueryCPU) as "Possible Pct of CPU Used for Extracts" /*  percentage of cpu dedicated to extracts */
		,'CB_DAILY_EXTRACT_WK_LOAD'
		,user
		,current_timestamp
 /* - */
FROM (	SELECT LogDate
		    , LogIORat
		    , PossExCPU
			, QueryCPU
		FROM ( 	select logdate
,sum(case when UII > 1e1 then TotalIOCount else 0e0 end) PossLogIO /* LogIO  intermediate */
,sum(TotalIOCOunt) QueryLogIO /* LogIO  intermediate */
,PossLogIO / QueryLogIO as LogIORat /* gIORat   "Extract Index" */
 /*  "Pct of Total CPU used by extracts" is the next column (PossExCPU) divided by the sum of the column QueryCPU */
		  			,sum(case when UII > 1e1 then AMPCPUTime else 0e0 end) as PossExCPU
		  			,sum(AMPCPUTime) as QueryCPU
 /*  "Pct of total IO used by extracts" is PosLogIO divided by the sum of column QueryLogIO */
				from (	Select logdate, AMPCPUTime ,TotalIOCount, ReqPhysIO, NumResultRows
		    				,Case when AMPCPUTime < 1e-1  or StatementType in all('Insert','Update','Delete','End loading','create table','checkpoint loading','help','collect statistics')
		                          or NumSteps = 0 or NumOfActiveAMPS = 0 then 0e0
		            		else TotalIOCount/(AMPCPUTime *1e3) end  as UII
		  				From  pdcrinfo.DBQLogTbl_hst
						where  logdate BETWEEN {ppstartdate} and {ppenddate}           /* Modify window as desired */
 /*  Since were looking for high UII< there has to be SOME AMP work involved */
		  				and NumSteps > 0 and AMPCPUTime > 0e0 and NumOfActiveAMPS > 0
 /*  Elminate BAR */
		  				and AppID <> 'DSMAIN' and UserName <> 'MOSDECODE02'
					) x1     /* Modify username pattern to exclude certain user ids */
		  			Group by 1
			) extracts
	) t1

	INNER JOIN CB_DAILY_TCORE_METRICS_RATIO t2
	ON t1.logdate = t2.logdate

	GROUP BY 1
;

UPDATE CB_DAILY_EXTRACT_WK
FROM (SELECT thedate, cast((sum(HostReadKB)/1000/1000) as float) as HostReadKB
	from PDCRINFO.resusagespma_hst where thedate between {ppstartdate} and {ppenddate}  group by 1) B
SET data_egress = b.HostReadKB
WHERE logdate = b.thedate
;


/* Summary EXTRACTS */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {prevdaterange} as DateRange
,'EXT'
,{ppstartdate} as Date_Start
,{ppenddate} as Date_End
,case when ({prevdaterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {ppenddate})
	  when ({prevdaterange} = 3) then 'MONTH'
	  when ({prevdaterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({prevdaterange} = 1) then 'LW'
	  when ({prevdaterange} = 3) then 'LM'
	  when ({prevdaterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'SUMMARY' as AGGREGATE_LEVEL
,Null as AGGREGATE_NAME
,'EXTRACTS'  as METRIC_NAME
,'Extracts Daily Average' as DASH_METRIC_NAME
,'Extracts T-Core' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,AVG(ExtractByTCore)  as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM	CB_DAILY_EXTRACT_WK  A
where A.LOGDATE between {ppstartdate} and {ppenddate}
;

/* Summary DATA EGRESS */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {prevdaterange} as DateRange
,'EGRESS'
,{ppstartdate} as Date_Start
,{ppenddate} as Date_End
,case when ({prevdaterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {ppenddate})
	  when ({prevdaterange} = 3) then 'MONTH'
	  when ({prevdaterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({prevdaterange} = 1) then 'LW'
	  when ({prevdaterange} = 3) then 'LM'
	  when ({prevdaterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'SUMMARY' as AGGREGATE_LEVEL
,Null as AGGREGATE_NAME
,'DATA EGRESS'  as METRIC_NAME
,'Data Egress Daily Average in GB' as DASH_METRIC_NAME
,'Data Egress (GB)' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,AVG(DATA_EGRESS)  as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM	CB_DAILY_EXTRACT_WK  A
where A.LOGDATE between {ppstartdate} and {ppenddate}
;

/* Summary DATA INGRESS */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {prevdaterange} as DateRange
,'INGRESS'
,{ppstartdate} as Date_Start
,{ppenddate} as Date_End
,case when ({prevdaterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {ppenddate})
	  when ({prevdaterange} = 3) then 'MONTH'
	  when ({prevdaterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({prevdaterange} = 1) then 'LW'
	  when ({prevdaterange} = 3) then 'LM'
	  when ({prevdaterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'SUMMARY' as AGGREGATE_LEVEL
,Null as AGGREGATE_NAME
,'DATA INGRESS'  as METRIC_NAME
,'Data Ingress Daily Average in GB' as DASH_METRIC_NAME
,'Data Ingress (GB)' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,AVG(HostWriteKB)/1000/1000 as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM (SELECT sum(HostWriteKB) as HostWriteKB, thedate from PDCRINFO.resusagespma_hst where thedate between {ppstartdate} and {ppenddate} group by 2) A
/*pdcrinfo.TableSpace_Hst A where A.LOGDATE between {ppstartdate} and {ppenddate}*/
;

/* Summary STORAGE */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {prevdaterange} as DateRange
,'STORAGE'
,{ppstartdate} as Date_Start
,{ppenddate} as Date_End
,case when ({prevdaterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {ppenddate})
	  when ({prevdaterange} = 3) then 'MONTH'
	  when ({prevdaterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({prevdaterange} = 1) then 'LW'
	  when ({prevdaterange} = 3) then 'LM'
	  when ({prevdaterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'SUMMARY' as AGGREGATE_LEVEL
,Null as AGGREGATE_NAME
,'STORAGE'  as METRIC_NAME
,'Storage Daily Average in TB' as DASH_METRIC_NAME
,'Storage (TB)' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
, AVG(AVG_CDS_TB) as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM	(
SELECT logdate,
       AVG(Object_cds) as Total_CDS,
       CAST((Total_CDS/1000.00) as float) as AVG_CDS_TB
    FROM(
    SELECT
	    b.logdate,
        b.databasename as Object_name,
        {currentpermThreshold} AS CurrentPerm_Threshold,
        {systemcds}  AS system_CDS,
        CAST(SUM(a.CurrentPerm) AS FLOAT)/1000/1000/1000 AS Object_CurrentPerm_GB,
        CAST(((Object_CurrentPerm_GB/CurrentPerm_Threshold)*System_CDS) AS Float) AS Object_CDS
        FROM dbc.diskspace a
	    JOIN pdcrinfo.DatabaseSpace_Hst b
        ON a.databasename=b.databasename
		where LOGDATE between {ppstartdate} and {ppenddate}
        GROUP BY 1,2,3,4) cds

		group by 1

 ) A
;

/* Clean Up Worktable before next */
delete from CB_DAILY_UTIL_WK
;

/* UTIL */
insert into CB_DAILY_UTIL_WK
select
(select sum(cast(TCOREUSAGE as dec(15,5))) as ttl from
CB_DAILY_QUERY_METRICS
where logdate between {ppstartdate} and {ppenddate}
) as ttl
,(select sum(cast(TCOREUSAGE as dec(15,5))) as answ from
CB_DAILY_QUERY_METRICS a JOIN
dim_app b ON a.appid = b.appid
where use_bucket IN ('Analytic', 'Application')
and statementtype not in ('collect statistics')
and logdate between {ppstartdate} and {ppenddate}
) as Answ
,(select sum(cast(TCOREUSAGE as dec(15,5))) as Main from
CB_DAILY_QUERY_METRICS
where statementtype  in ('collect statistics')
and logdate between {ppstartdate} and {ppenddate}
) as Main
,(select sum(cast(TCOREUSAGE as dec(15,5))) as ETL from
CB_DAILY_QUERY_METRICS a JOIN
dim_app b ON a.appid = b.appid
where use_bucket = 'ETL'
and logdate between {ppstartdate} and {ppenddate}
and statementtype not in ('collect statistics')
) as ETL
,cast(ttl - Answ - Main - ETL as bigint) as SysProc
,(select ZEROIFNULL(sum(cast(TCOREUSAGE as dec(15,5)))) as TTLWD from
CB_DAILY_QUERY_METRICS
where logdate between {ppstartdate} and {ppenddate}
) as TTLWD
,(select ZEROIFNULL(sum(cast(TCOREUSAGE as dec(15,5)))) as ANALYTIC from
CB_DAILY_QUERY_METRICS a JOIN
dim_app b ON a.appid = b.appid
where use_bucket = 'ANALYTIC'
and logdate between {ppstartdate} and {ppenddate}
) as ANALYTIC
,(select ZEROIFNULL(sum(cast(TCOREUSAGE as dec(15,5)))) as APPLICATION from
CB_DAILY_QUERY_METRICS a JOIN
dim_app b ON a.appid = b.appid
where use_bucket = 'APPLICATION'
and logdate between {ppstartdate} and {ppenddate}
) as APPLICATION
,(select ZEROIFNULL(sum(cast(TCOREUSAGE as dec(15,5)))) as STREAM from
CB_DAILY_QUERY_METRICS a JOIN
dim_app b ON a.appid = b.appid
where use_bucket = 'STREAM'
and logdate between {ppstartdate} and {ppenddate}
) as STREAM
,(select ZEROIFNULL(sum(cast(TCOREUSAGE as dec(15,5)))) as ETLWD from
CB_DAILY_QUERY_METRICS a JOIN
dim_app b ON a.appid = b.appid
where use_bucket = 'ETL'
and logdate between {ppstartdate} and {ppenddate}
) as ETLWD
,cast(TTLWD - ANALYTIC - APPLICATION - STREAM - ETLWD as bigint) as SysMgmt
,'CB_DAILY_UTIL_WK_LOAD'
,user
,current_timestamp
;

/* UTILIZATION Answers */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {prevdaterange} as DateRange
,'ANSW'
,{ppstartdate} as Date_Start
,{ppenddate} as Date_End
,case when ({prevdaterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {ppenddate})
	  when ({prevdaterange} = 3) then 'MONTH'
	  when ({prevdaterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({prevdaterange} = 1) then 'LW'
	  when ({prevdaterange} = 3) then 'LM'
	  when ({prevdaterange} = 5) then 'LQ'
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

/* UTILIZATION UTIL INGEST ETL */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {prevdaterange} as DateRange
,'ETL'
,{ppstartdate} as Date_Start
,{ppenddate} as Date_End
,case when ({prevdaterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {ppenddate})
	  when ({prevdaterange} = 3) then 'MONTH'
	  when ({prevdaterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({prevdaterange} = 1) then 'LW'
	  when ({prevdaterange} = 3) then 'LM'
	  when ({prevdaterange} = 5) then 'LQ'
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

/* UTILIZATION UTIL MAINTENANCE */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {prevdaterange} as DateRange
,'MAIN'
,{ppstartdate} as Date_Start
,{ppenddate} as Date_End
,case when ({prevdaterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {ppenddate})
	  when ({prevdaterange} = 3) then 'MONTH'
	  when ({prevdaterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({prevdaterange} = 1) then 'LW'
	  when ({prevdaterange} = 3) then 'LM'
	  when ({prevdaterange} = 5) then 'LQ'
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

/* UTILIZATION UTIL System/Procedural */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {prevdaterange} as DateRange
,'SysProc'
,{ppstartdate} as Date_Start
,{ppenddate} as Date_End
,case when ({prevdaterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {ppenddate})
	  when ({prevdaterange} = 3) then 'MONTH'
	  when ({prevdaterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({prevdaterange} = 1) then 'LW'
	  when ({prevdaterange} = 3) then 'LM'
	  when ({prevdaterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'UTILIZATION' as AGGREGATE_LEVEL
,NULL as AGGREGATE_NAME
,'UTIL SYSTEM/PROCEDURAL'  as METRIC_NAME
,'System/Procedural' as DASH_METRIC_NAME
,'System/Procedural' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,SysProc as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM	CB_DAILY_UTIL_WK
;

/* UTILIZATION Workload AdHoc */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {prevdaterange} as DateRange
,'Analytic'
,{ppstartdate} as Date_Start
,{ppenddate} as Date_End
,case when ({prevdaterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {ppenddate})
	  when ({prevdaterange} = 3) then 'MONTH'
	  when ({prevdaterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({prevdaterange} = 1) then 'LW'
	  when ({prevdaterange} = 3) then 'LM'
	  when ({prevdaterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'UTILIZATION' as AGGREGATE_LEVEL
,'Analytic' as AGGREGATE_NAME
,'UTIL WORKLOAD TCORE'  as METRIC_NAME
,'Workload T-Core Usage ' as DASH_METRIC_NAME
,'Workload T-Core' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,Analytic as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM	CB_DAILY_UTIL_WK
;

/* UTILIZATION Workload BI */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {prevdaterange} as DateRange
,'Application'
,{ppstartdate} as Date_Start
,{ppenddate} as Date_End
,case when ({prevdaterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {ppenddate})
	  when ({prevdaterange} = 3) then 'MONTH'
	  when ({prevdaterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({prevdaterange} = 1) then 'LW'
	  when ({prevdaterange} = 3) then 'LM'
	  when ({prevdaterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'UTILIZATION' as AGGREGATE_LEVEL
,'Application' as AGGREGATE_NAME
,'UTIL WORKLOAD TCORE'  as METRIC_NAME
,'Workload T-Core Usage ' as DASH_METRIC_NAME
,'Workload T-Core' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,Application as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM	CB_DAILY_UTIL_WK
;

insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {prevdaterange} as DateRange
,'Stream'
,{ppstartdate} as Date_Start
,{ppenddate} as Date_End
,case when ({prevdaterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {ppenddate})
	  when ({prevdaterange} = 3) then 'MONTH'
	  when ({prevdaterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({prevdaterange} = 1) then 'LW'
	  when ({prevdaterange} = 3) then 'LM'
	  when ({prevdaterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'UTILIZATION' as AGGREGATE_LEVEL
,'Stream' as AGGREGATE_NAME
,'UTIL WORKLOAD TCORE'  as METRIC_NAME
,'Workload T-Core Usage ' as DASH_METRIC_NAME
,'Workload T-Core' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,Stream as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM	CB_DAILY_UTIL_WK
;

insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {prevdaterange} as DateRange
,'ETLWD'
,{ppstartdate} as Date_Start
,{ppenddate} as Date_End
,case when ({prevdaterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {ppenddate})
	  when ({prevdaterange} = 3) then 'MONTH'
	  when ({prevdaterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({prevdaterange} = 1) then 'LW'
	  when ({prevdaterange} = 3) then 'LM'
	  when ({prevdaterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'UTILIZATION' as AGGREGATE_LEVEL
,'ETL' as AGGREGATE_NAME
,'UTIL WORKLOAD TCORE'  as METRIC_NAME
,'Workload T-Core Usage ' as DASH_METRIC_NAME
,'Workload T-Core' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,ETLWD as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM	CB_DAILY_UTIL_WK
;

insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {prevdaterange} as DateRange
,'SysMgmt'
,{ppstartdate} as Date_Start
,{ppenddate} as Date_End
,case when ({prevdaterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {ppenddate})
	  when ({prevdaterange} = 3) then 'MONTH'
	  when ({prevdaterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({prevdaterange} = 1) then 'LW'
	  when ({prevdaterange} = 3) then 'LM'
	  when ({prevdaterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'UTILIZATION' as AGGREGATE_LEVEL
,'Sys-Mgmt' as AGGREGATE_NAME
,'UTIL WORKLOAD TCORE'  as METRIC_NAME
,'Workload T-Core Usage ' as DASH_METRIC_NAME
,'Workload T-Core' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,SysMgmt as MEASURE_AMT
,0 as BENCHMARK_AMT,

	null as COMPLEXITY_SCORE,
	null as NBR_ACTIVE_USERS
FROM	CB_DAILY_UTIL_WK
;

/* DEPT */
Insert into CB_DAILY_DEPT_WK
Select
DEPARTMENT,
ActiveUser,
TCoreUsage,
ComplexityLevel,
ComplexityLevel/NULLIFZERO(ActiveUser),
'CB_DAILY_DEPT_WK_LOAD',
USER,
current_timestamp
from
(
select DEPARTMENT,
count(distinct Username) as ActiveUser,
sum(ComplexityLevel) as ComplexityLevel,
sum(cast(TCoreUsage as dec(38,10))) as TCoreUsage
FROM	CB_DAILY_QUERY_METRICS
where LOGDATE between {ppstartdate} and {ppenddate}
group by 1
) a
;

/* DEPT (Resource Sharing) */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {prevdaterange} as DateRange
,b.DEPARTMENT
,{ppstartdate} as Date_Start
,{ppenddate} as Date_End
,case when ({prevdaterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {ppenddate})
	  when ({prevdaterange} = 3) then 'MONTH'
	  when ({prevdaterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({prevdaterange} = 1) then 'LW'
	  when ({prevdaterange} = 3) then 'LM'
	  when ({prevdaterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'DEPT' as AGGREGATE_LEVEL
,b.DEPARTMENT as AGGREGATE_NAME
,'RESOURCE SHARING'  as METRIC_NAME
,'Resource Sharing' as DASH_METRIC_NAME
,'Resource Sharing' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,b.TCoreUsage / (select sum(TCoreUsage) from CB_DAILY_DEPT_WK)  as MEASURE_AMT
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
			sum(cast(TCoreUsage as dec(38,10))) as TCoreUsage
			from CB_DAILY_QUERY_METRICS A
			where a.logdate between {ppstartdate} and {ppenddate}
			group by 1

	) a




) b
;

/* DEPT (TCORE HR TOTAL) */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {prevdaterange} as DateRange
,b.DEPARTMENT
,{ppstartdate} as Date_Start
,{ppenddate} as Date_End
,case when ({prevdaterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {ppenddate})
	  when ({prevdaterange} = 3) then 'MONTH'
	  when ({prevdaterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({prevdaterange} = 1) then 'LW'
	  when ({prevdaterange} = 3) then 'LM'
	  when ({prevdaterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'DEPT' as AGGREGATE_LEVEL
,b.DEPARTMENT as AGGREGATE_NAME
,'TCORE HR TOTAL'  as METRIC_NAME
,'T-Core Hours Total' as DASH_METRIC_NAME
,'Total' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,cast(b.TCoreUsage as dec(38,10)) as MEASURE_AMT
,B.TCORE_BUDGET_HOURS as BENCHMARK_AMT
,NULL as COMPLEXITY_SCORE
,Null as NBR_ACTIVE_USERS
FROM (
	select
	DEPARTMENT,
	TCoreUsage,
	{tcorebudget} as TCORE_BUDGET_HOURS
	from
	(
			Select
			DEPARTMENT,
			sum(cast(TCoreUsage as dec(38,10))) as TCoreUsage
			from CB_DAILY_QUERY_METRICS A
			where a.logdate between {ppstartdate} and {ppenddate}
			group by 1

	) a



	left outer join
 (
select 50000 as TCORE_BUDGET_HOURS
/*select sum(TCORE_BUDGET_HOURS) as TCORE_BUDGET_HOURS
from CB_DIM_BUDGET_REF
where START_DT >=
(
    case when 2 between 1 and 4 then (select  MonthBegin from CB_DIM_BusinessCalendar where calendar_date = {ppstartdate})
	     when 2 between 5 and 6 then (select  ADD_MONTHS(QuarterBegin, (SELECT cast(COLUMNVALUE as int) FROM  CB_DIM_CONSTANT_REF WHERE COLUMNNAME='QTR_OFFSET')) from CB_DIM_BusinessCalendar where calendar_date = {ppstartdate})
	else {ppstartdate} end)
and END_DT <=
(
	case  when 2 between 1 and 4 then (select monthend from CB_DIM_BusinessCalendar where calendar_date = {ppenddate})
		  when 2 between 5 and 6 then (select monthend from CB_DIM_BusinessCalendar where calendar_date in  (select ADD_MONTHS(QuarterEnd, (SELECT cast(COLUMNVALUE as int) FROM  CB_DIM_CONSTANT_REF WHERE COLUMNNAME='QTR_OFFSET')) from CB_DIM_BusinessCalendar where calendar_date = {ppenddate}))
	else {ppenddate} end)*/
 ) B
	on 1=1


) b
;

/* DEPT  (Active Users) */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {prevdaterange} as DateRange
,b.DEPARTMENT
,{ppstartdate} as Date_Start
,{ppenddate} as Date_End
,case when ({prevdaterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {ppenddate})
	  when ({prevdaterange} = 3) then 'MONTH'
	  when ({prevdaterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({prevdaterange} = 1) then 'LW'
	  when ({prevdaterange} = 3) then 'LM'
	  when ({prevdaterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'DEPT' as AGGREGATE_LEVEL
,b.DEPARTMENT as AGGREGATE_NAME
,'ACTIVE USERS'  as METRIC_NAME
,'Active Users' as DASH_METRIC_NAME
,'Active Users' as DASH_METRIC_NAME_SHORT
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
			where A.LOGDATE between {ppstartdate} and {ppenddate}
			group by 1

	) a
) b
;

/* DEPT  (Nbr Queries) */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {prevdaterange} as DateRange
,b.DEPARTMENT
,{ppstartdate} as Date_Start
,{ppenddate} as Date_End
,case when ({prevdaterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {ppenddate})
	  when ({prevdaterange} = 3) then 'MONTH'
	  when ({prevdaterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({prevdaterange} = 1) then 'LW'
	  when ({prevdaterange} = 3) then 'LM'
	  when ({prevdaterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'DEPT' as AGGREGATE_LEVEL
,b.DEPARTMENT as AGGREGATE_NAME
,'NBR QUERIES'  as METRIC_NAME
,'Number of Queries' as DASH_METRIC_NAME
,'Queries' as DASH_METRIC_NAME_SHORT
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
			where A.LOGDATE between {ppstartdate} and {ppenddate}
			and queryid > 0
			group by 1

	) a
) b
;

/* DEPT  (NBR QUERIES GT1) */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 2 as DateRange
,b.DEPARTMENT
,{ppstartdate} as Date_Start
,{ppenddate} as Date_End
,case when ({prevdaterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {ppenddate})
	  when ({prevdaterange} = 3) then 'MONTH'
	  when ({prevdaterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({prevdaterange} = 1) then 'LW'
	  when ({prevdaterange} = 3) then 'LM'
	  when ({prevdaterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'DEPT' as AGGREGATE_LEVEL
,b.DEPARTMENT as AGGREGATE_NAME
,'NBR QUERIES GT1'  as METRIC_NAME
,'# Queries > 1 CPU Second' as DASH_METRIC_NAME
,'# Queries >1CPU' as DASH_METRIC_NAME_SHORT
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
			where A.LOGDATE between {ppstartdate} and {ppenddate}
			and queryid > 0
			and TotalCpu >= 1
			group by 1

	) a
) b
;

/* DEPT  (Peak Concurrency and AvgConcurry Worktable) */
INSERT INTO CB_DAILY_CONCURRENCY_DEPT_WK
SELECT
DEPARTMENT
,cast(StartTmHr as date) LogDate
,extract(HOUR from StartTmHr) as LogHour
,round(avg(PointConcurrency),0) as Concurrency_Avg
,max(PointConcurrency) as Concurrency_Peak
,'CB_DAILY_SUMMARY_CURR_PREV_LOAD' AS JOB_NM
,user AS USR_NM
,current_timestamp AS EDW_START_TSP
FROM
  (SELECT
   User_DEPARTMENT as Department,
   cast(SUBSTR(CAST(ClockTick AS  VARCHAR(30)), 1, 14) || '00:00' as timestamp(0))  StartTmHr
   , clockTick  /* Every 10 seconds */
   , SUM(QryCount)  PointConcurrency
   ,(row_number() OVER(PARTITION BY StartTmHr ORDER BY PointConcurrency)- 1) * 100
/ COUNT(*) OVER(PARTITION BY StartTmHr) AS Ntile /* tTmHr) AS Ntile   Ntile for the 600 10 second samples within the hour */
    FROM
        (  /* the expand  by anchor second clause duplicates the dbql columns for each second between the firststeptime and firstresptime.
            grouping on the second tells us how many concurrent queries were running during that second */
        SELECT   BEGIN(Qper)  ClockTick
    ,User_DEPARTMENT
        ,cast(SUBSTR(CAST(ClockTick AS  VARCHAR(30)), 1, 17) || '0'  as timestamp(0)) as StartTm10s
        , CAST(1 AS SMALLINT) QryCount
        , PERIOD(firststeptime,firstresptime+ interval '0.001' second) QryDurationPeriod
        FROM pdcrinfo.dbqlogtbl as lg

      left outer join dim_user QM

    on LG.USERNAME = QM.username



        WHERE logdate   BETWEEN  {ppstartdate}  AND {ppenddate}
          AND NumOfActiveAmps >  0
         EXPAND ON QryDurationPeriod AS Qper BY ANCHOR ANCHOR_SECOND
        ) qrylog
    WHERE  extract(second  from ClockTick) in (0,10,20,30,40,50)  /* GIVES 600 POINTS PER 1 HOUR INTERVAL SO NTILE DOESNT HAVE BIG EDGE EFFECT  */
    GROUP BY 1, 2, 3
  ) ex
GROUP BY 1,2,3
;

/* DEPT  (Peak Concurrency) */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {prevdaterange} as DateRange
,b.DEPARTMENT
,{ppstartdate} as Date_Start
,{ppenddate} as Date_End
,case when ({prevdaterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {ppenddate})
	  when ({prevdaterange} = 3) then 'MONTH'
	  when ({prevdaterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({prevdaterange} = 1) then 'LW'
	  when ({prevdaterange} = 3) then 'LM'
	  when ({prevdaterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'DEPT' as AGGREGATE_LEVEL
,b.DEPARTMENT as AGGREGATE_NAME
,'PEAK CONCURRENCY'  as METRIC_NAME
,'Peak Concurrency' as DASH_METRIC_NAME
,'Peak Concurrency' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,MAX(b.Concurrency_Peak) as MEASURE_AMT
,0 as BENCHMARK_AMT
,NULL as COMPLEXITY_SCORE
,NULL NBR_ACTIVE_USERS
FROM
(
SELECT	logdate, DEPARTMENT,max(concurrency_peak) as concurrency_peak
FROM	CB_DAILY_CONCURRENCY_DEPT_WK
group by 1,2
) b
GROUP BY DEPARTMENT
;

/* DEPT  (Concurrent Users) */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {prevdaterange} as DateRange
,b.DEPARTMENT
,{ppstartdate} as Date_Start
,{ppenddate} as Date_End
,case when ({prevdaterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {ppenddate})
	  when ({prevdaterange} = 3) then 'MONTH'
	  when ({prevdaterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({prevdaterange} = 1) then 'LW'
	  when ({prevdaterange} = 3) then 'LM'
	  when ({prevdaterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'DEPT' as AGGREGATE_LEVEL
,b.DEPARTMENT as AGGREGATE_NAME
,'CONCURRENT USERS'  as METRIC_NAME
,'Concurrent Users' as DASH_METRIC_NAME
,'Concurrent Users' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,round(avg(b.concurrency_avg))  as MEASURE_AMT
,0 as BENCHMARK_AMT
,NULL as COMPLEXITY_SCORE
,NULL NBR_ACTIVE_USERS
FROM (
SELECT	logdate, DEPARTMENT, sum(concurrency_avg) as concurrency_avg
FROM	CB_DAILY_CONCURRENCY_DEPT_WK
group by 1,2) b
GROUP BY DEPARTMENT
;

/* DEPT  (Complexity Score) */
insert into CB_DAILY_SUMMARY_CURR_PREV_WK
SELECT
 {prevdaterange} as DateRange
,b.DEPARTMENT
,{ppstartdate} as Date_Start
,{ppenddate} as Date_End
,case when ({prevdaterange} = 1) then (select
	case when (day_of_week = 1) then 'SUN'
	 when (day_of_week = 2) then 'MON'
	 when (day_of_week = 3) then 'TUE'
	 when (day_of_week = 4) then 'WED'
	 when (day_of_week = 5) then 'THU'
	 when (day_of_week = 6) then 'FRI'
	 ELSE  'SAT' END
from sys_calendar.BusinessCalendar where calendar_date = {ppenddate})
	  when ({prevdaterange} = 3) then 'MONTH'
	  when ({prevdaterange} = 5) then 'QTR'
	  Else ''
	  END as DASH_DATE_LEVEL

,case when ({prevdaterange} = 1) then 'LW'
	  when ({prevdaterange} = 3) then 'LM'
	  when ({prevdaterange} = 5) then 'LQ'
	  Else ''
	  END as DASH_DATE_LABEL
,'DEPT' as AGGREGATE_LEVEL
,b.DEPARTMENT as AGGREGATE_NAME
,'COMPLEXITY SCORE'  as METRIC_NAME
,'Complexity Score' as DASH_METRIC_NAME
,'Complexity' as DASH_METRIC_NAME_SHORT
,'' as DASH_METRIC_DESC
,b.ComplexityLevel as MEASURE_AMT
,0 as BENCHMARK_AMT
,Null as COMPLEXITY_SCORE
,Null as NBR_ACTIVE_USERS
FROM (
	select
	DEPARTMENT,
	case when (ComplexityScore > .5 ) then 4
		when (ComplexityScore between .3 and .5 ) then 3
		when (ComplexityScore between .05 and .2 ) then 2
		when (ComplexityScore > 0 ) then 1
		else 0 end as ComplexityLevel
	from
	(
			Select
			DEPARTMENT,
			(ComplexityScorePerUser - minComplexityScorePerUser) / nullif ((maxComplexityScorePerUser - minComplexityScorePerUser),0) as ComplexityScore

			from CB_DAILY_DEPT_WK A
			inner join (select max(ComplexityScorePerUser) as maxComplexityScorePerUser,
							min(ComplexityScorePerUser) as minComplexityScorePerUser
			from CB_DAILY_DEPT_WK) B



			on 1=1


	) a
) b
;

/* END OF PREVIOUS PERIOD */
CREATE MULTISET VOLATILE TABLE CB_DAILY_SUMMARY_CURR_PREV_MTH ,FALLBACK ,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO,
    MAP = TD_MAP1
    (
    DATE_CURR_START DATE FORMAT 'YYYY-MM-DD',
    DATE_CURR_END DATE FORMAT 'YYYY-MM-DD',
    DATE_PREV_START DATE FORMAT 'YYYY-MM-DD',
    DATE_PREV_END DATE FORMAT 'YYYY-MM-DD',
    DASH_DATE_LEVEL VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC,
    DASH_DATE_LABEL VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC,
    AGGREGATE_LEVEL VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC,
    AGGREGATE_NAME VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
    METRIC_NAME VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
    DASH_METRIC_NAME VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
    DASH_METRIC_NAME_SHORT VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
    DASH_METRIC_DESC VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
    MEASURE_AMT_CURR DECIMAL(38,10),
    MEASURE_AMT_PREV DECIMAL(38,10),
    CHANGE_CURR_PREV_PCT FLOAT,
    BENCHMARK_AMT_CURR FLOAT,
    BENCHMARK_AMT_PREV FLOAT,
    MEASURE_TO_BENCHMARK_CURR_PCT FLOAT,
    MEASURE_TO_BENCHMARK_PREV_PCT FLOAT,
    BENCHMARK_USAGE_RATING SMALLINT,
    COMPLEXITY_SCORE SMALLINT,
    NBR_ACTIVE_USERS INTEGER,
    JOB_NM VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
    USR_NM VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
    EDW_START_TSP TIMESTAMP(6) NOT NULL)
    PRIMARY INDEX ( DATE_CURR_START ,DATE_CURR_END ,DASH_DATE_LABEL ,
    AGGREGATE_LEVEL ,METRIC_NAME ) ON COMMIT PRESERVE ROWS
;

insert into CB_DAILY_SUMMARY_CURR_PREV_MTH
	select
		distinct
		 a.Date_Start as DATE_CURR_START
		,a.Date_End as DATE_CURR_END
		,b.Date_Start as DATE_PREV_START
		,b.Date_End as DATE_PREV_END
		,a.DASH_DATE_LEVEL as DASH_DATE_LEVEL
		,a.DASH_DATE_LABEL as  DASH_DATE_LABEL
		,a.AGGREGATE_LEVEL as  AGGREGATE_LEVEL
		,a.AGGREGATE_NAME as AGGREGATE_NAME
		,a.METRIC_NAME as METRIC_NAME
		,a.DASH_METRIC_NAME as DASH_METRIC_NAME
		,a.DASH_METRIC_NAME_SHORT as DASH_METRIC_NAME_SHORT
		,a.DASH_METRIC_DESC as DASH_METRIC_DESC
		,a.MEASURE_AMT as MEASURE_AMT_CURR
		,b.MEASURE_AMT as MEASURE_AMT_PREV
		,((a.MEASURE_AMT - b.MEASURE_AMT)/nullif (b.MEASURE_AMT,0))  as CHANGE_CURR_PREV_PCT
		,a.BENCHMARK_AMT as BENCHMARK_AMT_CURR
		,b.BENCHMARK_AMT as BENCHMARK_AMT_PREV
		,((a.BENCHMARK_AMT - a.MEASURE_AMT)/nullif (a.BENCHMARK_AMT,0))  as MEASURE_TO_BENCHMARK_CURR_PCT
		,((b.BENCHMARK_AMT - b.MEASURE_AMT)/nullif (b.BENCHMARK_AMT,0))  as MEASURE_TO_BENCHMARK_PREV_PCT
		,Case when (a.MEASURE_AMT <= (a.BENCHMARK_AMT/(select extract(day from MonthEnd) from  sys_calendar.BusinessCalendar where calendar_date in (select max(Date_End) from  CB_DAILY_SUMMARY_CURR_PREV_WK)))) then 1 else 0 end  as BENCHMARK_USAGE_RATING
		,a.COMPLEXITY_SCORE as COMPLEXITY_SCORE
		,a.NBR_ACTIVE_USERS as NBR_ACTIVE_USERS
		,'CB_DAILY_SUMMARY_CURR_PREV_LOAD'
		,USER
		,Current_timestamp

	from CB_DAILY_SUMMARY_CURR_PREV_WK a
		inner join  CB_DAILY_SUMMARY_CURR_PREV_WK b
		on a.MatchNM= b.MatchNm
		and a.DateRange = 1 and a.AGGREGATE_LEVEL <> 'DEPT'
		and b.DateRange = 2 and b.AGGREGATE_LEVEL <> 'DEPT'
union all
	select
		distinct
		 a.Date_Start as DATE_CURR_START
		,a.Date_End as DATE_CURR_END
		,b.Date_Start as DATE_PREV_START
		,b.Date_End as DATE_PREV_END
		,a.DASH_DATE_LEVEL as DASH_DATE_LEVEL
		,a.DASH_DATE_LABEL as  DASH_DATE_LABEL
		,a.AGGREGATE_LEVEL as  AGGREGATE_LEVEL
		,a.AGGREGATE_NAME as AGGREGATE_NAME
		,a.METRIC_NAME as METRIC_NAME
		,a.DASH_METRIC_NAME as DASH_METRIC_NAME
		,a.DASH_METRIC_NAME_SHORT as DASH_METRIC_NAME_SHORT
		,a.DASH_METRIC_DESC as DASH_METRIC_DESC
		,a.MEASURE_AMT as MEASURE_AMT_CURR
		,b.MEASURE_AMT as MEASURE_AMT_PREV
		,((a.MEASURE_AMT - b.MEASURE_AMT)/nullif(b.MEASURE_AMT,0))  as CHANGE_CURR_PREV_PCT
		,a.BENCHMARK_AMT as BENCHMARK_AMT_CURR
		,b.BENCHMARK_AMT as BENCHMARK_AMT_PREV
		,0 as MEASURE_TO_BENCHMARK_CURR_PCT
		,0 as MEASURE_TO_BENCHMARK_PREV_PCT
		,0  as BENCHMARK_USAGE_RATING
		,a.COMPLEXITY_SCORE as COMPLEXITY_SCORE
		,a.NBR_ACTIVE_USERS as NBR_ACTIVE_USERS
		,'CB_DAILY_SUMMARY_CURR_PREV_LOAD'
		,USER
		,Current_timestamp
	 from CB_DAILY_SUMMARY_CURR_PREV_WK a
		left outer join CB_DAILY_SUMMARY_CURR_PREV_WK b
		on a.MatchNM = b.MatchNm
		and a.Metric_name = b.Metric_name
		and a.AGGREGATE_LEVEL = 'DEPT'
		and a.DateRange = 1
		and b.DateRange = 2
		where
		a.DASH_DATE_LABEL= 'LW'
		and a.AGGREGATE_LEVEL = 'DEPT'
union all
	select
		distinct
		 a.Date_Start as DATE_CURR_START
		,a.Date_End as DATE_CURR_END
		,b.Date_Start as DATE_PREV_START
		,b.Date_End as DATE_PREV_END
		,a.DASH_DATE_LEVEL as DASH_DATE_LEVEL
		,a.DASH_DATE_LABEL as  DASH_DATE_LABEL
		,a.AGGREGATE_LEVEL as  AGGREGATE_LEVEL
		,a.AGGREGATE_NAME as AGGREGATE_NAME
		,a.METRIC_NAME as METRIC_NAME
		,a.DASH_METRIC_NAME as DASH_METRIC_NAME
		,a.DASH_METRIC_NAME_SHORT as DASH_METRIC_NAME_SHORT
		,a.DASH_METRIC_DESC as DASH_METRIC_DESC
		,a.MEASURE_AMT as MEASURE_AMT_CURR
		,b.MEASURE_AMT as MEASURE_AMT_PREV
		,((a.MEASURE_AMT - b.MEASURE_AMT)/nullif (b.MEASURE_AMT,0)) as CHANGE_CURR_PREV_PCT
		,a.BENCHMARK_AMT as BENCHMARK_AMT_CURR
		,b.BENCHMARK_AMT as BENCHMARK_AMT_PREV
		,((a.BENCHMARK_AMT - a.MEASURE_AMT)/nullif (a.BENCHMARK_AMT,0)) as MEASURE_TO_BENCHMARK_CURR_PCT
		,((b.BENCHMARK_AMT - b.MEASURE_AMT)/nullif (b.BENCHMARK_AMT,0))  as MEASURE_TO_BENCHMARK_PREV_PCT
		,Case when (a.MEASURE_AMT <= (a.BENCHMARK_AMT/(select extract(day from MonthEnd) from  sys_calendar.BusinessCalendar where calendar_date in (select max(Date_End) from  CB_DAILY_SUMMARY_CURR_PREV_WK))) * (select extract(day from calendar_date) from  sys_calendar.BusinessCalendar where calendar_date in (select max(Date_End) from CB_DAILY_SUMMARY_CURR_PREV_WK))) then 1 else 0 end  as BENCHMARK_USAGE_RATING
		,a.COMPLEXITY_SCORE as COMPLEXITY_SCORE
		,a.NBR_ACTIVE_USERS as NBR_ACTIVE_USERS
		,'CB_DAILY_SUMMARY_CURR_PREV_LOAD'
		,USER
		,Current_timestamp

	from CB_DAILY_SUMMARY_CURR_PREV_WK a
		inner join   CB_DAILY_SUMMARY_CURR_PREV_WK b
		on a.MatchNM= b.MatchNm
		and a.DateRange = 3 and a.AGGREGATE_LEVEL <> 'DEPT'
		and b.DateRange = 4 and b.AGGREGATE_LEVEL <> 'DEPT'
union all
	select
		distinct
		 a.Date_Start as DATE_CURR_START
		,a.Date_End as DATE_CURR_END
		,b.Date_Start as DATE_PREV_START
		,b.Date_End as DATE_PREV_END
		,a.DASH_DATE_LEVEL as DASH_DATE_LEVEL
		,a.DASH_DATE_LABEL as  DASH_DATE_LABEL
		,a.AGGREGATE_LEVEL as  AGGREGATE_LEVEL
		,a.AGGREGATE_NAME as AGGREGATE_NAME
		,a.METRIC_NAME as METRIC_NAME
		,a.DASH_METRIC_NAME as DASH_METRIC_NAME
		,a.DASH_METRIC_NAME_SHORT as DASH_METRIC_NAME_SHORT
		,a.DASH_METRIC_DESC as DASH_METRIC_DESC
		,a.MEASURE_AMT as MEASURE_AMT_CURR
		,b.MEASURE_AMT as MEASURE_AMT_PREV
		,((a.MEASURE_AMT - b.MEASURE_AMT)/nullif(b.MEASURE_AMT,0))   as CHANGE_CURR_PREV_PCT
		,a.BENCHMARK_AMT as BENCHMARK_AMT_CURR
		,b.BENCHMARK_AMT as BENCHMARK_AMT_PREV
		,0 as MEASURE_TO_BENCHMARK_CURR_PCT
		,0 as MEASURE_TO_BENCHMARK_PREV_PCT
		,0  as BENCHMARK_USAGE_RATING
		,a.COMPLEXITY_SCORE as COMPLEXITY_SCORE
		,a.NBR_ACTIVE_USERS as NBR_ACTIVE_USERS
		,'CB_DAILY_SUMMARY_CURR_PREV_LOAD'
		,USER
		,Current_timestamp
	from CB_DAILY_SUMMARY_CURR_PREV_WK a
		left outer join CB_DAILY_SUMMARY_CURR_PREV_WK b
		on a.MatchNM = b.MatchNm
		and a.Metric_name = b.Metric_name
		and a.AGGREGATE_LEVEL = 'DEPT'
		and a.DateRange = 3
		and b.DateRange = 4
		where
		a.DASH_DATE_LEVEL= 'MONTH'
		and a.AGGREGATE_LEVEL = 'DEPT'
union all
	select
		distinct
		 a.Date_Start as DATE_CURR_START
		,a.Date_End as DATE_CURR_END
		,b.Date_Start as DATE_PREV_START
		,b.Date_End as DATE_PREV_END
		,a.DASH_DATE_LEVEL as DASH_DATE_LEVEL
		,a.DASH_DATE_LABEL as  DASH_DATE_LABEL
		,a.AGGREGATE_LEVEL as  AGGREGATE_LEVEL
		,a.AGGREGATE_NAME as AGGREGATE_NAME
		,a.METRIC_NAME as METRIC_NAME
		,a.DASH_METRIC_NAME as DASH_METRIC_NAME
		,a.DASH_METRIC_NAME_SHORT as DASH_METRIC_NAME_SHORT
		,a.DASH_METRIC_DESC as DASH_METRIC_DESC
		,a.MEASURE_AMT as MEASURE_AMT_CURR
		,b.MEASURE_AMT as MEASURE_AMT_PREV
		,((a.MEASURE_AMT - b.MEASURE_AMT)/nullif (b.MEASURE_AMT,0))   as CHANGE_CURR_PREV_PCT
		,a.BENCHMARK_AMT as BENCHMARK_AMT_CURR
		,b.BENCHMARK_AMT as BENCHMARK_AMT_PREV
		,((a.BENCHMARK_AMT - a.MEASURE_AMT)/nullif (a.BENCHMARK_AMT,0))  as MEASURE_TO_BENCHMARK_CURR_PCT
		,((b.BENCHMARK_AMT - b.MEASURE_AMT)/nullif (b.BENCHMARK_AMT,0))  as MEASURE_TO_BENCHMARK_PREV_PCT
		,Case when (a.MEASURE_AMT <= (a.BENCHMARK_AMT/(select extract(day from MonthEnd) from  sys_calendar.BusinessCalendar where calendar_date in (select max(Date_End) from  CB_DAILY_SUMMARY_CURR_PREV_WK))) * (select extract(day from calendar_date) from  sys_calendar.BusinessCalendar where calendar_date in (select max(Date_End) from CB_DAILY_SUMMARY_CURR_PREV_WK))) then 1 else 0 end  as BENCHMARK_USAGE_RATING
		,a.COMPLEXITY_SCORE as COMPLEXITY_SCORE
		,a.NBR_ACTIVE_USERS as NBR_ACTIVE_USERS
		,'CB_DAILY_SUMMARY_CURR_PREV_LOAD'
		,USER
		,Current_timestamp

	from CB_DAILY_SUMMARY_CURR_PREV_WK a
		inner join   CB_DAILY_SUMMARY_CURR_PREV_WK b
		on a.MatchNM= b.MatchNm
		and a.DateRange = 5 and a.AGGREGATE_LEVEL <> 'DEPT'
		and b.DateRange = 6 and b.AGGREGATE_LEVEL <> 'DEPT'
union all
	select
		distinct
		 a.Date_Start as DATE_CURR_START
		,a.Date_End as DATE_CURR_END
		,b.Date_Start as DATE_PREV_START
		,b.Date_End as DATE_PREV_END
		,a.DASH_DATE_LEVEL as DASH_DATE_LEVEL
		,a.DASH_DATE_LABEL as  DASH_DATE_LABEL
		,a.AGGREGATE_LEVEL as  AGGREGATE_LEVEL
		,a.AGGREGATE_NAME as AGGREGATE_NAME
		,a.METRIC_NAME as METRIC_NAME
		,a.DASH_METRIC_NAME as DASH_METRIC_NAME
		,a.DASH_METRIC_NAME_SHORT as DASH_METRIC_NAME_SHORT
		,a.DASH_METRIC_DESC as DASH_METRIC_DESC
		,a.MEASURE_AMT as MEASURE_AMT_CURR
		,b.MEASURE_AMT as MEASURE_AMT_PREV
		,((a.MEASURE_AMT - b.MEASURE_AMT)/nullif(b.MEASURE_AMT,0))   as CHANGE_CURR_PREV_PCT
		,a.BENCHMARK_AMT as BENCHMARK_AMT_CURR
		,b.BENCHMARK_AMT as BENCHMARK_AMT_PREV
		,0 as MEASURE_TO_BENCHMARK_CURR_PCT
		,0 as MEASURE_TO_BENCHMARK_PREV_PCT
		,0  as BENCHMARK_USAGE_RATING
		,a.COMPLEXITY_SCORE as COMPLEXITY_SCORE
		,a.NBR_ACTIVE_USERS as NBR_ACTIVE_USERS
		,'CB_DAILY_SUMMARY_CURR_PREV_LOAD'
		,USER
		,Current_timestamp
	from CB_DAILY_SUMMARY_CURR_PREV_WK a
		left outer join CB_DAILY_SUMMARY_CURR_PREV_WK b
		on a.MatchNM = b.MatchNm
		and a.Metric_name = b.Metric_name
		and a.AGGREGATE_LEVEL = 'DEPT'
		and a.DateRange = 5
		and b.DateRange = 6
		where
		a.DASH_DATE_LEVEL= 'QTR'
		and a.AGGREGATE_LEVEL = 'DEPT'
;

/*{{save:CB_Daily_Summmary_Curr_Prev.csv}}*/
/*{{load:adlste_coa_stg.Stg_CB_DAILY_SUMMARY_CURR_PREV}}*/
/*{{call:adlste_coa.sp_dat_CB_DAILY_SUMMARY_CURR_PREV('{fileset_version}')}}*/
select '{siteid}' as Site_ID,
    DATE_CURR_START (FORMAT 'yyyy-mm-dd'),
    DATE_CURR_END (FORMAT 'yyyy-mm-dd'),
    DATE_PREV_START (FORMAT 'yyyy-mm-dd'),
    DATE_PREV_END (FORMAT 'yyyy-mm-dd'),
    DASH_DATE_LEVEL,
    DASH_DATE_LABEL,
    AGGREGATE_LEVEL,
    AGGREGATE_NAME,
    METRIC_NAME,
    DASH_METRIC_NAME,
    DASH_METRIC_NAME_SHORT,
    DASH_METRIC_DESC,
    MEASURE_AMT_CURR,
    MEASURE_AMT_PREV,
    CHANGE_CURR_PREV_PCT,
    BENCHMARK_AMT_CURR,
    BENCHMARK_AMT_PREV,
    MEASURE_TO_BENCHMARK_CURR_PCT,
    MEASURE_TO_BENCHMARK_PREV_PCT,
    BENCHMARK_USAGE_RATING,
    COMPLEXITY_SCORE,
    NBR_ACTIVE_USERS,
    JOB_NM,
    USR_NM,
    EDW_START_TSP
	  from CB_DAILY_SUMMARY_CURR_PREV_MTH
;

CREATE MULTISET VOLATILE TABLE CB_DAILY_CONCURRENCY ,FALLBACK ,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO,
    MAP = TD_MAP1
    (
    DEPARTMENT VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
    LogDate DATE FORMAT 'yyyy-mm-dd',
    LogHour INTEGER,
    Concurrency_Avg FLOAT,
    Concurrency_Peak INTEGER,
    JOB_NM VARCHAR(31) CHARACTER SET UNICODE NOT CASESPECIFIC,
    USR_NM VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
    EDW_START_TSP TIMESTAMP(6) WITH TIME ZONE)
    PRIMARY INDEX ( DEPARTMENT ,LogDate ,LogHour ) ON COMMIT PRESERVE ROWS
;

INSERT INTO CB_DAILY_CONCURRENCY
SELECT
DEPARTMENT
,cast(StartTmHr as date) LogDate
,extract(HOUR from StartTmHr) as LogHour
,round(avg(PointConcurrency),0) as Concurrency_Avg
,max(PointConcurrency) as Concurrency_Peak
,'CB_DAILY_SUMMARY_CURR_PREV_LOAD' AS JOB_NM
,user AS USR_NM
,current_timestamp AS EDW_START_TSP
FROM
  (SELECT
   DEPARTMENT,
   cast(SUBSTR(CAST(ClockTick AS  VARCHAR(30)), 1, 14) || '00:00' as timestamp(0))  StartTmHr
   , clockTick  /* Every 10 seconds */
   , SUM(QryCount)  PointConcurrency
   ,(row_number() OVER(PARTITION BY StartTmHr ORDER BY PointConcurrency)- 1) * 100
/ COUNT(*) OVER(PARTITION BY StartTmHr) AS Ntile /* tTmHr) AS Ntile   Ntile for the 600 10 second samples within the hour */
    FROM
        (  /* the expand  by anchor second clause duplicates the dbql columns for each second between the firststeptime and firstresptime.
            grouping on the second tells us how many concurrent queries were running during that second */
        SELECT   BEGIN(Qper)  ClockTick
		,User_DEPARTMENT as Department
        ,cast(SUBSTR(CAST(ClockTick AS  VARCHAR(30)), 1, 17) || '0'  as timestamp(0)) as StartTm10s
        , CAST(1 AS SMALLINT) QryCount
        , case when   ( firststeptime< firstresptime+ interval '0.001' second)  then  PERIOD(firststeptime,firstresptime + interval '0.001' second) else PERIOD (firststeptime,firststeptime+ interval '0.001' second) end QryDurationPeriod
        FROM pdcrinfo.dbqlogtbl as lg

			left outer join dim_user QM

		on LG.USERNAME = QM.username



        WHERE logdate BETWEEN  {cpstartdate} and {cpenddate}
          AND NumOfActiveAmps >  0
         EXPAND ON QryDurationPeriod AS Qper BY ANCHOR ANCHOR_SECOND
        ) qrylog
    WHERE  extract(second  from ClockTick) in (0,10,20,30,40,50)  /* GIVES 600 POINTS PER 1 HOUR INTERVAL SO NTILE DOESN'T HAVE BIG EDGE EFFECT  */
    GROUP BY 1, 2, 3
  ) ex
GROUP BY 1,2,3
;

CREATE MULTISET VOLATILE TABLE CB_DAILY_SUMMARY_DEPT_USER_HR ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      SITE_ID VARCHAR(128) CHARACTER SET LATIN NOT CASESPECIFIC,
	  LOGDATE DATE FORMAT 'YYYY-MM-DD',
      USERNAME VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
      FIRST_NAME VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      LAST_NAME VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      EMAIL_ADDR VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      REGION VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      LOCATION VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      DEPARTMENT VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      SUBDEPARTMENT_L1 VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      SUBDEPARTMENT_L2 VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      SUBDEPARTMENT_L3 VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
	  DIRECT_MGR VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      L5_MGR_NM VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      L4_MGR_NM VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      L3_MGR_NM VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      L2_MGR_NM VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      EMPLOYEE_STATUS VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
      LOGHOUR INTEGER,
      ACTIVEUSER INTEGER,
      QUERYCNTGT INTEGER,
      QUERYCNTLT INTEGER,
      TOTALIOCNT FLOAT,
      TOTALCPU DECIMAL(15,7),
      TCOREUSAGE DECIMAL(15,10),
      PEAKCURR INTEGER,
      CONCUSER INTEGER,
      BENCHMARK_AMT INTEGER,
      JOB_NM VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
      USR_NM VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
      EDW_START_TSP TIMESTAMP(6) NOT NULL)
PRIMARY INDEX ( LOGDATE ,USERNAME ,DEPARTMENT ) ON COMMIT PRESERVE ROWS
;

insert into CB_DAILY_SUMMARY_DEPT_USER_HR
select
'{siteid}' as SITE_ID,
logdate,
username,
FIRST_NAME,
LAST_NAME,
EMAIL_ADDR,
REGION,
LOCATION,
DEPARTMENT,
SUBDEPARTMENT_L1,
SUBDEPARTMENT_L2,
SUBDEPARTMENT_L3,
DIRECT_MGR,
L5_MGR_NM,
L4_MGR_NM,
L3_MGR_NM,
L2_MGR_NM,
EMPLOYEE_STATUS,
LogHour,
count(distinct (x.username)) as activeuser,
sum(querycntGT) as QueryCntGT,
sum(querycntLT) as QueryCntLT,
sum(TotalIOCount) as TotalIOCnt,
sum(TotalCPU) as TotalCPU,
sum(TCoreUsage) as TCoreUsage,
Max(PeakCurr) as PeakCurr,
round(avg(ConcUser)) as ConcUser,
min(TCORE_BUDGET_HOURS) as BENCHMARK_AMT,
'CB_DAILY_SUMMARY_DEPT_USER_HR_LOAD' as JOB_NM,
USER as USR_NM,
Current_timestamp(6) as EDW_START_TSP
from
(
	SELECT
				 l.logdate,
				 l.FIRST_NAME          ,
				 l.LAST_NAME           ,
				 l.EMAIL_ADDR          ,
				 l.REGION          ,
				 l.LOCATION          ,
				 l.DEPARTMENT          ,
				 l.SUBDEPARTMENT_L1       ,
				 l.SUBDEPARTMENT_L2       ,
				 l.SUBDEPARTMENT_L3       ,

				 l.DIRECT_MGR          ,
				 l.L5_MGR_NM          ,
				 l.L4_MGR_NM          ,
				 l.L3_MGR_NM          ,
				 l.L2_MGR_NM          ,
				 l.EMPLOYEE_STATUS ,
				 l.username as username,
				 l.loghour as loghour,
                 Case when (totalcpu >= 1) then 1 else 0 end as querycntGT,
				 Case when (totalcpu < 1) then 1 else 0 end as querycntLT,
                 l.TotalIOCount,
                 l.TotalCPU,
                 l.TCoreUsage,
				 {tcorebudget} as TCORE_BUDGET_HOURS,
				 c.Concurrency_Peak as PeakCurr,
				 c.Concurrency_Avg as ConcUser
   FROM    CB_DAILY_QUERY_METRICS l

	/*left outer join TD_Consumption_DB_BASE.CB_DIM_BUDGET_REF B
	on b.Budget_year = Extract(Year from l.logdate)
	and b.Budget_Month =  Extract(Month from l.logdate)	*/
	left outer join  CB_DAILY_CONCURRENCY C
	on l.logdate = c.logdate
	and l.loghour = c.loghour
	and coalesce(l.DEPARTMENT,'Unknown')=C.DEPARTMENT


	where l.logdate between add_months({cpenddate}, -1) and {cpenddate} - 1 ) x

	group by
		logdate,
		username,
		FIRST_NAME,
		LAST_NAME,
		EMAIL_ADDR,
		REGION,
		LOCATION,
		DEPARTMENT,
		SUBDEPARTMENT_L1,
		SUBDEPARTMENT_L2,
		SUBDEPARTMENT_L3,
		DIRECT_MGR,
		L5_MGR_NM,
		L4_MGR_NM,
		L3_MGR_NM,
		L2_MGR_NM,
		EMPLOYEE_STATUS,
		LogHour
;

CREATE MULTISET VOLATILE TABLE CB_DAILY_SUMMARY_DEPT_USER_DAY ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      SITE_ID VARCHAR(128) CHARACTER SET LATIN NOT CASESPECIFIC,
	  LOGDATE DATE FORMAT 'YYYY-MM-DD',
      USERNAME VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
      FIRST_NAME VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      LAST_NAME VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      EMAIL_ADDR VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      REGION VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      LOCATION VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      DEPARTMENT VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      SUBDEPARTMENT_L1 VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      SUBDEPARTMENT_L2 VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      SUBDEPARTMENT_L3 VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      DIRECT_MGR VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      L5_MGR_NM VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      L4_MGR_NM VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      L3_MGR_NM VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      L2_MGR_NM VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
      EMPLOYEE_STATUS VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC,
      ACTIVEUSER INTEGER,
      QUERYCNTGT INTEGER,
      QUERYCNTLT INTEGER,
      TOTALIOCNT FLOAT,
      TOTALCPU DECIMAL(15,7),
      TCOREUSAGE DECIMAL(15,10),
	  TCOREUSAGE_MTD DECIMAL(25,10),
      PEAKCURR INTEGER,
      CONCUSER INTEGER,
      BENCHMARK_AMT INTEGER,
	  BENCHMARK_YR_AMT INTEGER,
      JOB_NM VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
      USR_NM VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
      EDW_START_TSP TIMESTAMP(6) NOT NULL)
PRIMARY INDEX ( LOGDATE ,USERNAME ,DEPARTMENT ) ON COMMIT PRESERVE ROWS
;

INSERT INTO CB_DAILY_SUMMARY_DEPT_USER_DAY
select
	'{siteid}' as SITE_ID,
	logdate,
	username,
	FIRST_NAME,
	LAST_NAME,
	EMAIL_ADDR,
	REGION,
	LOCATION,
	DEPARTMENT,
	SUBDEPARTMENT_L1,
	SUBDEPARTMENT_L2,
	SUBDEPARTMENT_L3,
	DIRECT_MGR,
	L5_MGR_NM,
	L4_MGR_NM,
	L3_MGR_NM,
	L2_MGR_NM,
	EMPLOYEE_STATUS,
	activeuser,
	QueryCntGT,
	QueryCntLT,
	TotalIOCnt,
	TotalCPU,
	TCoreUsage,
	sum(cast(TCoreUsage as float)) OVER (partition by extract (month from LogDate)  ORDER BY logdate ROWS UNBOUNDED PRECEDING) AS TCoreUsage_MTD,
	PeakCurr,
	ConcUser,
	BENCHMARK_AMT,
	BENCHMARK_YR_AMT,
	'CB_DAILY_SUMMARY_DEPT_USER_HR_LOAD' as JOB_NM,
	USER as USR_NM,
	Current_timestamp(6) as EDW_START_TSP
from
(
select
logdate,
username,
FIRST_NAME,
LAST_NAME,
EMAIL_ADDR,
REGION,
LOCATION,
DEPARTMENT,
SUBDEPARTMENT_L1,
SUBDEPARTMENT_L2,
SUBDEPARTMENT_L3,
DIRECT_MGR,
L5_MGR_NM,
L4_MGR_NM,
L3_MGR_NM,
L2_MGR_NM,
EMPLOYEE_STATUS,
count(distinct (x.username)) as activeuser,
sum(querycntGT) as QueryCntGT,
sum(querycntLT) as QueryCntLT,
sum(TotalIOCount) as TotalIOCnt,
sum(TotalCPU) as TotalCPU,
sum(TCoreUsage) as TCoreUsage,
Max(PeakCurr) as PeakCurr,
round(avg(ConcUser)) as ConcUser,
min(TCORE_BUDGET_HOURS) as BENCHMARK_AMT,
min(BUDGET_YR) as BENCHMARK_YR_AMT
from
(
	SELECT
				 l.logdate,
				 l.FIRST_NAME          ,
				 l.LAST_NAME           ,
				 l.EMAIL_ADDR          ,
				 l.REGION          ,
				 l.LOCATION          ,
				 l.DEPARTMENT          ,
				 l.SUBDEPARTMENT_L1       ,
				 l.SUBDEPARTMENT_L2       ,
				 l.SUBDEPARTMENT_L3       ,

				 l.DIRECT_MGR          ,
				 l.L5_MGR_NM          ,
				 l.L4_MGR_NM          ,
				 l.L3_MGR_NM          ,
				 l.L2_MGR_NM          ,
				 l.EMPLOYEE_STATUS ,
				 l.username as username,
 /* 		 l.collecttimestamp as collecttimestamp, */
                 Case when (totalcpu >= 1) then 1 else 0 end as querycntGT,
				 Case when (totalcpu < 1) then 1 else 0 end as querycntLT,
                 l.TotalIOCount,
                 l.TotalCPU,
                 l.TCoreUsage,
				 {tcorebudget} as TCORE_BUDGET_HOURS /*TCORE_BUDGET_HOURS*/,
				 {tcorebudget} as BUDGET_YR /*d.BUDGET_YR*/,
				 c.Concurrency_Peak as PeakCurr,
				 c.Concurrency_Avg as ConcUser




   FROM    CB_DAILY_QUERY_METRICS l

	/*left outer join CB_DIM_BUDGET_REF B
	on b.Budget_year = Extract(Year from l.logdate)
	and b.Budget_Month =  Extract(Month from l.logdate)	*/
	left outer join CB_DAILY_CONCURRENCY C
	on l.logdate = c.logdate
	and l.loghour = c.loghour
	and coalesce(l.DEPARTMENT,'Unknown')=C.DEPARTMENT

	/*left outer join
	(select
	sum(TCORE_BUDGET_HOURS) as BUDGET_YR
	from
	TD_Consumption_DB_BASE.CB_DIM_BUDGET_REF
	where  START_DT>=  (select  ADD_MONTHS(YearBegin, (SELECT cast(COLUMNVALUE as int) FROM  TD_Consumption_DB_BASE.CB_DIM_CONSTANT_REF WHERE COLUMNNAME='QTR_OFFSET')) from TD_Consumption_DB_BASE.CB_DIM_BusinessCalendar where calendar_date = :DTE - 1)
	and end_dt <= (select  ADD_MONTHS(YearEnd, (SELECT cast(COLUMNVALUE as int) FROM  TD_Consumption_DB_BASE.CB_DIM_CONSTANT_REF WHERE COLUMNNAME='QTR_OFFSET')) from TD_Consumption_DB_BASE.CB_DIM_BusinessCalendar where calendar_date = :DTE - 1)
	) d
	on 1=1*/

	where l.logdate between add_months(DATE {cpenddate} - 1, -6) and DATE {cpenddate} - 1
) x

	group by
		logdate,
		username,
		FIRST_NAME,
		LAST_NAME,
		EMAIL_ADDR,
		REGION,
		LOCATION,
		DEPARTMENT,
		SUBDEPARTMENT_L1,
		SUBDEPARTMENT_L2,
		SUBDEPARTMENT_L3,
		DIRECT_MGR,
		L5_MGR_NM,
		L4_MGR_NM,
		L3_MGR_NM,
		L2_MGR_NM,
		EMPLOYEE_STATUS
) a
;

/*{{save:CB_DAILY_SUMMARY_DEPT_USER_HR.csv}}*/
/*{{load:adlste_coa_stg.Stg_CB_DAILY_SUMMARY_DEPT_USER_HR}}*/
/*{{call:adlste_coa.sp_dat_CB_DAILY_SUMMARY_DEPT_USER_HR('{fileset_version}')}}*/
SELECT
	SITE_ID,
	logdate (FORMAT 'yyyy-mm-dd'),
	username,
	FIRST_NAME,
	LAST_NAME,
	EMAIL_ADDR,
	REGION,
	LOCATION,
	DEPARTMENT,
	SUBDEPARTMENT_L1,
	SUBDEPARTMENT_L2,
	SUBDEPARTMENT_L3,
	DIRECT_MGR,
	L5_MGR_NM,
	L4_MGR_NM,
	L3_MGR_NM,
	L2_MGR_NM,
	EMPLOYEE_STATUS,
	LogHour,
	activeuser,
	QueryCntGT,
	QueryCntLT,
	TotalIOCnt,
	TotalCPU,
	TCoreUsage,
	PeakCurr,
	ConcUser,
	BENCHMARK_AMT,
	JOB_NM,
	USR_NM,
	EDW_START_TSP
	FROM CB_DAILY_SUMMARY_DEPT_USER_HR
;

/*{{save:CB_DAILY_SUMMARY_DEPT_USER_DAY.csv}}*/
/*{{load:adlste_coa_stg.stg_CB_DAILY_SUMMARY_DEPT_USER_DAY}}*/
/*{{call:adlste_coa.sp_dat_CB_DAILY_SUMMARY_DEPT_USER_DAY('{fileset_version}')}}*/
SELECT
      SITE_ID,
	  LOGDATE (FORMAT 'yyyy-mm-dd'),
      USERNAME,
      FIRST_NAME,
      LAST_NAME,
      EMAIL_ADDR,
      REGION,
      LOCATION,
      DEPARTMENT,
      SUBDEPARTMENT_L1,
      SUBDEPARTMENT_L2,
      SUBDEPARTMENT_L3,
      DIRECT_MGR,
      L5_MGR_NM,
      L4_MGR_NM,
      L3_MGR_NM,
      L2_MGR_NM,
      EMPLOYEE_STATUS,
      ACTIVEUSER,
      QUERYCNTGT,
      QUERYCNTLT,
      TOTALIOCNT,
      TOTALCPU,
      TCOREUSAGE,
	  TCOREUSAGE_MTD,
      PEAKCURR ,
      CONCUSER ,
      BENCHMARK_AMT ,
	  BENCHMARK_YR_AMT ,
      JOB_NM,
      USR_NM,
      EDW_START_TSP
	  FROM CB_DAILY_SUMMARY_DEPT_USER_DAY
;


CREATE MULTISET VOLATILE TABLE CB_DAILY_APP_FEAT_USAGE_WK ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      SITEID VARCHAR(11) CHARACTER SET UNICODE NOT CASESPECIFIC,
      LOGDATE DATE FORMAT 'YYYY-MM-DD',
      USERNAME VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
      APPGROUP VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC,
      ANALYTICS_EXTENSIONS BIGINT,
      ARRAY_DATA_TYPE BIGINT,
      COLUMNAR BIGINT,
      DATASET_DATA_TYPE BIGINT,
      DOT_NOTATION BIGINT,
      EXTERNAL_STORED_PROCEDURE BIGINT,
      GEOSPATIAL BIGINT,
      JOIN_INDEX BIGINT,
      JSON_DATA_TYPE BIGINT,
      LEAD_LAG_ANALYTICS BIGINT,
      LOB BIGINT,
      OBJECT_LOGGING BIGINT,
      ODBC_SCALAR_FUNCTIONS BIGINT,
      PARTITIONING BIGINT,
      PERIOD_DATA_TYPE BIGINT,
      QUERYBAND BIGINT,
      QUERYGRID BIGINT,
      R_TABLE_OPERATOR BIGINT,
      SCRIPT_TABLE_OPERATOR BIGINT,
      STRUCTURE_DATA_TYPE BIGINT,
      TABLE_FUNCTION BIGINT,
      TABLE_OPERATOR BIGINT,
      TERADATA_PIVOT BIGINT,
      TERADATA_REMOTE_QUERY BIGINT,
      TERADATA_STORED_PROCEDURE BIGINT,
      TERADATA_TEMPORAL BIGINT,
      TERADATA_UNPIVOT BIGINT,
      TIME_SERIES_TABLE BIGINT,
      TRIGGERS BIGINT,
      USER_DEFINED_FUNCTION BIGINT,
      USER_DEFINED_TYPE BIGINT,
      XML_DATA_TYPE BIGINT,
	  	JOB_NM VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
      USR_NM VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
      EDW_START_TSP TIMESTAMP(6) NOT NULL)
PRIMARY INDEX (LOGDATE, USERNAME) ON COMMIT PRESERVE ROWS
;

INSERT INTO CB_DAILY_APP_FEAT_USAGE_WK
SELECT
	'{siteid}' AS SiteID,
    LogDate,
    trim(cast(from_bytes(hashrow( d.Username),'base16') as char(9))) ||
         trim(cast(from_bytes(hashrow( substr(d.Username,1,3)  ),'base16') as char(9))) ||
         trim(cast(from_bytes(hashrow( substr(d.Username,floor(character_length(d.Username)/2)-1,3) ),'base16') as char(9))) ||
         trim(cast(from_bytes(hashrow( substr(d.Username,character_length(d.Username)-3,3)   ),'base16') as char(9))) as UserName,
    CASE
        WHEN AppID LIKE ANY ('ALTERYX%') THEN 'Alteryx'
        WHEN AppID LIKE ANY ('FASTEXP%') THEN 'FASTEXPORT'
        WHEN AppID LIKE ANY ('FASTLOAD%') THEN 'FASTLOAD'
        WHEN AppID LIKE ANY ('MULTLOAD%') THEN 'MULTILOAD'
        WHEN AppID LIKE ANY ('TPUMP%', 'TPUMPEXE%') THEN 'TPUMP'
        WHEN AppID LIKE ANY ('BTEQ%') THEN 'BTEQ'
        WHEN AppID LIKE ANY ('CSPPEEK%', '%TSET%', 'DUL%', 'DBGCRNR%', 'COLLECTION%') THEN 'TD Diagnostic Tools'
        WHEN AppID LIKE ANY ('BUSOBJ%', 'DESIGNER%','WEBIRICHCLIENT%', 'WIREPORTSERVER%', '%INFORMATIONDESIGNTOOL%', 'SQLCON32%', 'CONNECTIONSERVER%') THEN 'SAP BusinessObjects'
        WHEN AppID LIKE ANY ('JOBRUNNER%') THEN 'D3'
        WHEN AppID LIKE ANY ('EXECUTOR%') THEN 'Dell Boomi'
        WHEN AppID LIKE ANY ('AL_ENGINE%') THEN 'SAP Data Services'
        WHEN AppID LIKE ANY ('ODBC%') THEN 'ODBC'
        WHEN AppID LIKE ANY ('DTE%', 'DTS%', 'ISSERVEREXEC%') THEN 'MSFT SSIS'
        WHEN AppID LIKE ANY ('PL%','PERL%') THEN 'PERL'
        WHEN AppID LIKE ANY ('EXCEL', 'MSACCESS%', 'MSQRY32%', 'OUTLOOK', 'EXCEL%') THEN 'MSFT Office'
        WHEN AppID LIKE ANY ('JAVA%', 'JDBC%') THEN 'Java / JDBC'
        WHEN ClientID LIKE ANY ('UBUNTU%') THEN 'JDBC - PGP'
        WHEN AppID LIKE ANY ('PYTHON%', '%PYTHON%') THEN 'Python'
        WHEN AppID LIKE ANY ('QUERYMAN', 'SQLA%') THEN 'TD SQL Assistant'
        WHEN AppID LIKE ANY ('QUERYCHAMELEON%', 'CDW.ADMIN.BATCH%') THEN 'Nexus'
        WHEN AppID LIKE ANY ('SAS') THEN 'SAS'
        WHEN AppID LIKE ANY ('%DBEAVER%') THEN 'DBEAVER - JDBC'
        WHEN AppID LIKE ANY ('ACLDA64%') THEN 'Galvanize'
        WHEN AppID LIKE ANY ('Unavailable%') THEN 'TDWM'
        WHEN AppID LIKE ANY ('TABPROTO%') THEN 'Tableau'
        WHEN AppID LIKE ANY ('TABULAREDITOR%') THEN 'Tablular Editor - SSAS'
        WHEN AppID LIKE ANY ('TBUILDEXE%', 'TDLOADEXE%', 'TPTEXP%', 'TPTLOAD%', 'TPTUPD%', 'TPTSTRM%') THEN 'TD TPT'
        WHEN AppID LIKE ANY ('DEVENV%', 'IISEX%') THEN 'MSFT Visual Studio'
        WHEN AppID LIKE ANY ('ARCMAIN%', 'DSMAIN%') THEN 'TD Backup'
        WHEN AppID LIKE ANY ('QUICKFLOW%') THEN 'SFDC'
        WHEN AppID LIKE ANY ('ESSSVR%', 'OLAPISVR%') THEN 'Oracle Hyperion Essbase'
        WHEN AppID LIKE ANY ('SQLSERVR', 'SQLSERVR%', 'DNC%', '%Dllhost%') THEN 'MSFT SQL Server'
        WHEN AppID LIKE ANY ('MSMDSRV%') THEN 'MSFT SSAS'
        WHEN AppID LIKE ANY ('SSMS%') THEN 'MSFT SSMS'
        WHEN AppID LIKE ANY ('DIAWP%') THEN 'MSFT Azure Data Factory'
        WHEN AppID LIKE ANY ('SPOTFIRE%') THEN 'SPOTFIRE'
        WHEN AppID LIKE ANY ('TOAD%') THEN 'TOAD'
        WHEN AppID LIKE ANY ('MYSQLD%') THEN 'MySQL'
        WHEN AppID LIKE ANY ('MICROSOFT.R.HOST%') THEN 'MSFT R'
        WHEN AppID LIKE ANY ('RSCRIPT%', 'RSESSION%', 'RTERM%', 'RGUI%') THEN 'R Studio'
        WHEN AppID LIKE ANY ('PMDTM%', 'PMDTMSVC2%', '%PMDESIGN%') THEN 'Informatica'
        WHEN AppID LIKE ANY ('DOTNET%', '%NET:SS:%','%NET:S%','%DELL.MYDEAL.QUEUE.TERADATA:NET%','DELL.ACCESSREQUEST%', 'DELL.ACCESSREQUEST.TD1.SCHEDUL%', 'DELL.ACCESSREQUEST.TD1.REVOKE.%', 'POWERSHELL%') THEN '.net provider'
        WHEN AppID LIKE ANY ('DDW_%') THEN 'Abinitio'
        WHEN AppID LIKE ANY ('AAPLAYER%', 'AATASKEDITOR%','%AAWORKBENCH%') THEN 'Automation Anywhere'
        WHEN AppID LIKE ANY ('OLELOAD%') THEN 'OLE DB'
        WHEN AppID LIKE ANY ('BRIDGERUNNERC%') THEN 'Aster - JDBC'
        WHEN AppID LIKE ANY ('PHP%') THEN 'PHP - ODBC'
        WHEN AppID LIKE ANY ('LEAD_TIME_W%') THEN 'Apache Airflow'
        WHEN AppID LIKE ANY ('WINDDI') THEN 'TD Administrator'
        WHEN AppID LIKE ANY ('MICROSOFT.MASHUP%','MICROSOFT.POWERBI%','PREVIEWPROCESSINGSERVICE%', 'REPORTINGSERVICESSERVICE%') THEN 'MSFT PowerBI'
        WHEN ClientID LIKE ANY ('MICROSOFT.MASHUP%','MICROSOFT.POWERBI%','MICROSOFT.MASHUP.CONTAINER.NET') THEN 'MSFT PowerBI'
                   ELSE 'other'
    END AS AppGroup,

	(ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 100))) AS BIGINT)))*5 AS Analytics_Extensions,
    (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 49))) AS BIGINT)))*1 AS Array_Data_Type,
    (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 24))) AS BIGINT)))*5 AS Columnar,
    (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 126))) AS BIGINT)))*3+ (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 107))) AS BIGINT)))*3 AS Dataset_Data_Type,
    (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 108))) AS BIGINT)))*1 AS Dot_Notation,
    (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 53))) AS BIGINT)))*1 AS External_Stored_Procedure,
    (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 47))) AS BIGINT)))*10+ (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 124))) AS BIGINT)))*1+ (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 46))) AS BIGINT)))*1+ (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 123))) AS BIGINT)))*1+ (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 125))) AS BIGINT)))*1+ (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 122))) AS BIGINT)))*1 AS Geospatial,
    (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 87))) AS BIGINT)))*1 AS Join_Index,
    (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 43))) AS BIGINT)))*3 AS JSON_Data_Type,
    (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 121))) AS BIGINT)))*1 AS LEAD_LAG_Analytics,
    (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 37))) AS BIGINT)))*1+ (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 36))) AS BIGINT)))*1 AS LOB,
    (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 63))) AS BIGINT)))*1+ (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 67))) AS BIGINT)))*1+ (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 62))) AS BIGINT)))*1+ (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 68))) AS BIGINT)))*1+ (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 69))) AS BIGINT)))*1+ (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 65))) AS BIGINT)))*1+ (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 61))) AS BIGINT)))*1+ (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 64))) AS BIGINT)))*1+ (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 60))) AS BIGINT)))*1+ (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 66))) AS BIGINT)))*1 AS Object_Logging,
    (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 136))) AS BIGINT)))*1 AS ODBC_Scalar_Functions,
    (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 33))) AS BIGINT)))*1+ (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 28))) AS BIGINT)))*1+ (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 30))) AS BIGINT)))*1+ (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 29))) AS BIGINT)))*1+ (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 31))) AS BIGINT)))*1+ (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 27))) AS BIGINT)))*1+ (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 32))) AS BIGINT)))*1 AS Partitioning,
    (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 48))) AS BIGINT)))*1 AS Period_Data_Type,
    (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 77))) AS BIGINT)))*1 AS Queryband,
    (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 132))) AS BIGINT)))*1+ (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 133))) AS BIGINT)))*1+ (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 58))) AS BIGINT)))*10 AS QueryGrid,
    (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 98))) AS BIGINT)))*5 AS R_Table_Operator,
    (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 106))) AS BIGINT)))*7 AS Script_Table_Operator,
    (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 45))) AS BIGINT)))*3 AS Structure_Data_Type,
    (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 54))) AS BIGINT)))*3 AS Table_Function,
    (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 55))) AS BIGINT)))*5 AS Table_Operator,
    (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 74))) AS BIGINT)))*1 AS Teradata_Pivot,
    (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 57))) AS BIGINT)))*1 AS Teradata_Remote_Query,
    (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 51))) AS BIGINT)))*1 AS Teradata_Stored_Procedure,
    (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 34))) AS BIGINT)))*5 AS Teradata_Temporal,
    (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 75))) AS BIGINT)))*1 AS Teradata_Unpivot,
    (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 129))) AS BIGINT)))*5 AS Time_Series_Table,
    (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 21))) AS BIGINT)))*1 AS Triggers,
    (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 52))) AS BIGINT)))*3 AS User_Defined_Function,
    (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 41))) AS BIGINT)))*1 AS User_Defined_Type,
    (ZEROIFNULL(CAST(SUM(GETBIT(A.FEATUREUSAGE,(2047 - 42))) AS BIGINT)))*3 AS XML_Data_Type,
	'Application_Feature_Usage_Job_WK' AS JOB_NM,
	USER AS USR_NM,
	CURRENT_TIMESTAMP AS EDW_START_TSP

    FROM PDCRINFO.DBQLOGTBL_HST A
	left outer join dim_user d

	on d.USERNAME = A.username

	WHERE LogDate  BETWEEN {cpstartdate} and {cpenddate}

	GROUP BY 1,2,3,4
;

CREATE MULTISET VOLATILE TABLE CB_DAILY_APP_FEAT_USAGE ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      SITEID VARCHAR(11) CHARACTER SET UNICODE NOT CASESPECIFIC,
      LOGDATE DATE FORMAT 'YYYY-MM-DD',
      USERNAME VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
      APPGROUP VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC,
      TOOLGROUP VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC,
      APP_FEAT_COUNT BIGINT,
      APP_FEAT_NAME VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC,
      JOB_NM VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
      USR_NM VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
      EDW_START_TSP TIMESTAMP(6) NOT NULL
      )
PRIMARY INDEX (LOGDATE, USERNAME) ON COMMIT PRESERVE ROWS
;

INSERT INTO CB_DAILY_APP_FEAT_USAGE
SELECT
	SITEID,
	LOGDATE,
	USERNAME,
	APPGROUP,

	CASE
        WHEN APPGROUP LIKE ANY ('TD BACKUP') THEN 'Admin - DR'
        WHEN APPGROUP LIKE ANY ('TD DIAGNOSTIC TOOLS','TDWM') THEN 'Admin - TD'
        WHEN APPGROUP LIKE ANY ('D3','MSFT POWERBI','MSFT SSAS','SAP BUSINESSOBJECTS') THEN 'BI Tool'
        WHEN APPGROUP LIKE ANY ('Alteryx','Aster - JDBC','Galvanize','MSFT R','R Studio') THEN 'Data Science Tool'
        WHEN APPGROUP LIKE ANY ('MSFT SQL Server','MySQL') THEN 'Database'
        WHEN APPGROUP LIKE ANY ('BTEQ','FASTEXPORT','FASTLOAD','Informatica','JDBC - PGP','MSFT SSIS','MULTILOAD','SAP Data Services','TD TPT','TPUMP') THEN 'ETL Tool'
        WHEN APPGROUP LIKE ANY ('Apache Airflow','Automation Anywhere','Dell Boomi','MSFT Azure Data Factory') THEN 'Integration Tool'
        WHEN APPGROUP LIKE ANY ('.net provider','Java / JDBC','ODBC','OLE DB','PERL','PHP - ODBC','Python') THEN 'Language/API'
        WHEN APPGROUP LIKE ANY ('MSFT Office') THEN 'Productivity Tools'
        WHEN APPGROUP LIKE ANY ('DBEAVER - JDBC','MSFT SSMS','MSFT Visual Studio','Nexus','TD SQL Assistant','TOAD') THEN 'SQL Editor'
        WHEN APPGROUP LIKE ANY ('Tableau') THEN 'Visualization Tool'

        ELSE 'Other'
  	END AS ToolGroup,

	APP_FEAT_COUNT,
	APP_FEAT_NAME,
	'Application_Feature_Usage_Job_WK' AS JOB_NM,
	USER AS USR_NM,
	CURRENT_TIMESTAMP AS EDW_START_TSP

FROM TD_UNPIVOT
	(
		ON
			(SELECT * FROM CB_DAILY_APP_FEAT_USAGE_WK
				WHERE LOGDATE BETWEEN {cpstartdate} and {cpenddate}
				)
		USING
			VALUE_COLUMNS('APP_FEAT_COUNT')
			UNPIVOT_COLUMN('APP_FEAT_NAME')
			COLUMN_LIST('ANALYTICS_EXTENSIONS','ARRAY_DATA_TYPE', 'COLUMNAR', 'DATASET_DATA_TYPE', 'DOT_NOTATION', 'EXTERNAL_STORED_PROCEDURE','GEOSPATIAL', 'JOIN_INDEX', 'JSON_DATA_TYPE', 'LEAD_LAG_ANALYTICS', 'LOB','OBJECT_LOGGING', 'ODBC_SCALAR_FUNCTIONS', 'PARTITIONING', 'PERIOD_DATA_TYPE','QUERYBAND', 'QUERYGRID', 'R_TABLE_OPERATOR', 'SCRIPT_TABLE_OPERATOR','STRUCTURE_DATA_TYPE', 'TABLE_FUNCTION', 'TABLE_OPERATOR','TERADATA_PIVOT','TERADATA_REMOTE_QUERY', 'TERADATA_STORED_PROCEDURE', 'TERADATA_TEMPORAL','TERADATA_UNPIVOT','TIME_SERIES_TABLE','TRIGGERS','USER_DEFINED_FUNCTION','USER_DEFINED_TYPE','XML_DATA_TYPE')
	)X
WHERE
	APP_FEAT_COUNT > 0
;

CREATE MULTISET VOLATILE TABLE CB_DAILY_APP_FEAT_NONUSAGE ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      SITEID VARCHAR(11) CHARACTER SET UNICODE NOT CASESPECIFIC,
      USAGE_DATE DATE FORMAT 'YYYY-MM-DD',
      ACCOUNT_TYPE VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
      APPGROUP VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC,
      TOOLGROUP VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC,
      APP_FEAT_COUNT BIGINT,
      APP_FEAT_NAME VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC,
      JOB_NM VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
      USR_NM VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
      EDW_START_TSP TIMESTAMP(6) NOT NULL
      )
PRIMARY INDEX (USAGE_DATE, ACCOUNT_TYPE) ON COMMIT PRESERVE ROWS
;

INSERT INTO CB_DAILY_APP_FEAT_NONUSAGE
SELECT
	SITEID,
	USAGE_DATE,
	ACCOUNT_TYPE,
	NULL,
	NULL,
	APP_FEAT_COUNT,
	APP_FEAT_NAME,
	'Application_Feature_NonUsage_Job' AS JOB_NM,
	USER AS USR_NM,
	CURRENT_TIMESTAMP AS EDW_START_TSP

FROM TD_UNPIVOT
	(
		ON
			(
			SELECT
				SITEID,
				SUBSTR(CAST (LOGDATE AS CHAR(10)),1,8) || '01' AS USAGE_DATE,
				CASE
				        WHEN USERNAME LIKE ANY ('%SERVICE%','%DSS%','%SVC%','%ETL%') THEN 'SERVICE'
						WHEN USERNAME LIKE ANY ('%STATSMANAGER%','%PDCRADMIN%','%BASE_OWNER%','%GIS_WW%') THEN 'ADMIN'
						ELSE 'USER'
					END AS ACCOUNT_TYPE,

				SUM(ANALYTICS_EXTENSIONS) AS ANALYTICS_EXTENSIONS,
				SUM(ARRAY_DATA_TYPE) AS ARRAY_DATA_TYPE,
				SUM(COLUMNAR) AS COLUMNAR,
				SUM(DATASET_DATA_TYPE) AS DATASET_DATA_TYPE,
				SUM(DOT_NOTATION) AS DOT_NOTATION,
				SUM(EXTERNAL_STORED_PROCEDURE) AS EXTERNAL_STORED_PROCEDURE,
				SUM(GEOSPATIAL) AS GEOSPATIAL,
				SUM(JOIN_INDEX) AS JOIN_INDEX,
				SUM(JSON_DATA_TYPE) AS JSON_DATA_TYPE,
				SUM(LEAD_LAG_ANALYTICS) AS LEAD_LAG_ANALYTICS,
				SUM(LOB) AS LOB,
				SUM(OBJECT_LOGGING) AS OBJECT_LOGGING,
				SUM(ODBC_SCALAR_FUNCTIONS) AS ODBC_SCALAR_FUNCTIONS,
				SUM(PARTITIONING) AS PARTITIONING,
				SUM(PERIOD_DATA_TYPE) AS PERIOD_DATA_TYPE,
				SUM(QUERYBAND) AS QUERYBAND,
				SUM(QUERYGRID) AS QUERYGRID,
				SUM(R_TABLE_OPERATOR) AS R_TABLE_OPERATOR,
				SUM(SCRIPT_TABLE_OPERATOR) AS SCRIPT_TABLE_OPERATOR,
				SUM(STRUCTURE_DATA_TYPE) AS STRUCTURE_DATA_TYPE,
				SUM(TABLE_FUNCTION) AS TABLE_FUNCTION,
				SUM(TABLE_OPERATOR) AS TABLE_OPERATOR,
				SUM(TERADATA_PIVOT) AS TERADATA_PIVOT,
				SUM(TERADATA_REMOTE_QUERY) AS TERADATA_REMOTE_QUERY,
				SUM(TERADATA_STORED_PROCEDURE) AS TERADATA_STORED_PROCEDURE,
				SUM(TERADATA_TEMPORAL) AS TERADATA_TEMPORAL,
				SUM(TERADATA_UNPIVOT) AS TERADATA_UNPIVOT,
				SUM(TIME_SERIES_TABLE) AS TIME_SERIES_TABLE,
				SUM(TRIGGERS) AS TRIGGERS,
				SUM(USER_DEFINED_FUNCTION) AS USER_DEFINED_FUNCTION,
				SUM(USER_DEFINED_TYPE) AS USER_DEFINED_TYPE,
				SUM(XML_DATA_TYPE) AS XML_DATA_TYPE

				FROM CB_DAILY_APP_FEAT_USAGE_WK

WHERE LogDate     BETWEEN  {cpstartdate} AND {cpenddate} /* 7'       BETWEEN '2020-05-01' AND '2020-09-22' */
				GROUP BY 1,2,3

			)
		USING
			VALUE_COLUMNS('APP_FEAT_COUNT')
			UNPIVOT_COLUMN('APP_FEAT_NAME')
			COLUMN_LIST('ANALYTICS_EXTENSIONS','ARRAY_DATA_TYPE', 'COLUMNAR', 'DATASET_DATA_TYPE', 'DOT_NOTATION', 'EXTERNAL_STORED_PROCEDURE','GEOSPATIAL', 'JOIN_INDEX', 'JSON_DATA_TYPE', 'LEAD_LAG_ANALYTICS', 'LOB','OBJECT_LOGGING', 'ODBC_SCALAR_FUNCTIONS', 'PARTITIONING', 'PERIOD_DATA_TYPE','QUERYBAND', 'QUERYGRID', 'R_TABLE_OPERATOR', 'SCRIPT_TABLE_OPERATOR','STRUCTURE_DATA_TYPE', 'TABLE_FUNCTION', 'TABLE_OPERATOR','TERADATA_PIVOT','TERADATA_REMOTE_QUERY', 'TERADATA_STORED_PROCEDURE', 'TERADATA_TEMPORAL','TERADATA_UNPIVOT','TIME_SERIES_TABLE','TRIGGERS','USER_DEFINED_FUNCTION','USER_DEFINED_TYPE','XML_DATA_TYPE')
	)X

	WHERE APP_FEAT_COUNT = 0
	GROUP BY 1,2,3,4,5,6,7
;

/*{{save:CB_DAILY_APP_FEAT_USAGE.csv}}*/
/*{{load:adlste_coa_stg.Stg_CB_DAILY_APP_FEAT_USAGE}}*/
/*{{call:adlste_coa.sp_dat_CB_DAILY_APP_FEAT_USAGE('{fileset_version}')}}*/
SELECT
      SITEID as Site_ID,
      LOGDATE (FORMAT 'yyyy-mm-dd'),
      USERNAME,
      APPGROUP,
      TOOLGROUP,
      APP_FEAT_COUNT,
      APP_FEAT_NAME,
      JOB_NM,
      USR_NM,
      EDW_START_TSP
	  FROM CB_DAILY_APP_FEAT_USAGE
;

/*{{save:CB_DAILY_APP_FEAT_NONUSAGE.csv}}*/
/*{{load:adlste_coa_stg.Stg_CB_DAILY_APP_FEAT_NONUSAGE}}*/
/*{{call:adlste_coa.sp_dat_CB_DAILY_APP_FEAT_NONUSAGE('{fileset_version}')}}*/
SELECT
      SITEID as Site_ID,
      USAGE_DATE (FORMAT 'YYYY-MM-DD'),
      ACCOUNT_TYPE,
      APPGROUP,
      TOOLGROUP,
      APP_FEAT_COUNT,
      APP_FEAT_NAME,
      JOB_NM ,
      USR_NM,
      EDW_START_TSP
	  FROM CB_DAILY_APP_FEAT_NONUSAGE
;
