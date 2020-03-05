
/*===== Description ===== ===== ===== ===== =====

CPU Trend Spma AvgCPUBusyPct
DBC Option (uses PDCRINFO.ResusageSpma_hst)
Version 11 (2019-05-07)
Parameters:
  resusagespma    = {resusagespma}
  siteid          = {siteid}
  startdate       = {startdate}
  enddate         = {enddate}

Stage Table:  adlste_westcomm.consumption_cpu_forecast_stg
Stored Proc:  adlste_westcomm.consumption_cpu_forecast_sp('{fileset_version}')
Target Table: adlste_westcomm.consumption_cpu_forecast_v2


CPU Utilization 4-Hour Variable Peak from ResusageSpma (Viewpoint CPU Utilization Method).
•	Evaluates the percentage of time CPU’s in the collection period that CPU’s were busy processing requests (CPUUServ + CPUUExec ??CPUNice??
•	AvgCPUBusyPct is an average of CPU utilization across the entire system. The premise is that when CPU’s reach 80% busy (i.e., reserve capacity level) the system will likely suffer performance impact.
•	Evaluates 365 days of history (weekdays only) and the busiest 4-hours of each day (whenever they occur).  Peak periods are contiguous 4-hour periods (not individual hours) and may vary (PeakStart & PeakEnd displayed in result) depending on utilization.
•	Peak periods may start on one day and end in the next – peak periods are always recorded on the day in which they end.
•	Simple linear regression is used to determine trend line with slope & intercept.
•	The slope of the trend line is used to extend anticipated usage 365 days into the future or when utilization is forecasted to exceed 100% (whichever comes first).
•	21 day Moving Average is included to emphasize recent activity in addition to the longer trend (typically 21 business days in a calendar month).
•	Reserve Capacity is set at 80% (workload performance will likely be impacted when CPU’s exceed 80% utilization).
•	Reserve Horizon represents the (future) point in time at which utilization is expected to exceed 80%.
•	Slope is the daily percentage increase/decrease in utilization of the trend line (positive = increasing utilization, negative = decreasing utilization).
•	SQL uses UNION to combine historical trend with future forecast – identical changes typically must be made to both SQL statements in UNION.
•
Execution Instructions
•	Copy/Paste below query into favorite query tool & execute.
•	Copy/Paste result set from query tool into Excel and save as .xls or .xlsx
o	Don’t export results (impacts formatting).
o	Don’t save results to spreadsheeting (impacts formatting).
•	Use visualization tool to import results for analysis (coming soon).

Using the results to drive customer discussions
•	Trending up or down? What is the growth rate (monthly change in percent utilization)?
•	How long before trend hits 80% (reserve horizon) and 100% maximum?
•	What happens when actual utilization hits the reserve amount (~80%)
o	Performance becomes more variable as more requests compete for limited resources
o	Delay queue gets longer, and response time increases for the same type of requests
o	Tactical queries may miss SLA’s without proper workload management rules
o	Customer submits more Performance P1’s – “queries are slow – my system is broken!”
•	Teradata can help address the risk of not having enough resources to meet workload demand
o	Release COD or system expansion
o	Is there enough time for performance optimization (probably requires 3 to 6 month runway).


Hard-Coded parameters
Changes can be made by modifying the SQL below (change the yellow highlighted values):
(2) Reserve Capacity – default is 80% of utilization (only one change required)
,80 AS ReserveX
(3) Variable Peak Length (N+1) in hours (value 3 = 4 hours) – two changes required
,AVG(HourlyAvgIOPct) OVER (ORDER BY  SiteID, TheDate ,TheHour ROWS 3 PRECEDING) AS VPeakAvgIOPct
(5) Future forecast - number of days into the future for extended trend line – two changes required.
WHERE  c2.calendar_date BETWEEN a5.TheDate+1 AND a5.TheDate + 365


===== SQL ===== ===== ===== ===== =====*/

/*{{save:{siteid}--cpu_trend_spma.coa.csv}}*/
/*{{load:adlste_westcomm.consumption_cpu_forecast_stg}}*/
/*{{call:adlste_westcomm.consumption_cpu_forecast_sp('{fileset_version}')}}*/
LOCK ROW FOR ACCESS
SELECT
'{siteid}' as SiteID /* Enter the Customer SiteID */
,Current_Date (format'YYYY-MM-DD') (CHAR(10)) as "Report Date"
,TheDate(format'YYYY-MM-DD') (CHAR(10)) as "Log Date"
,PeakStart ||':00:00' as "Peak Start"
,PeakEnd ||':00:00' as "Peak End"
,AvgCPUPct (DECIMAL(18,4)) as "Avg CPU Pct"
,CASE
 WHEN Period_Number < 21 THEN NULL
 WHEN AvgCPUPct IS NULL THEN NULL
 ELSE MovingAvg END (DECIMAL(18,4)) AS "Moving Avg"
,Trend (DECIMAL(18,4)) as Trend
,ReserveX
,CASE WHEN Trend >= ReserveX THEN Trend ELSE NULL END (DECIMAL(18,4)) AS "Reserve Horizon"
,SlopeX (DECIMAL(18,4)) as SlopeX
FROM
(
SELECT
 SiteID
,COUNT(*) OVER (ORDER BY calendar_date ROWS UNBOUNDED PRECEDING ) AS Period_Number
,Calendar_Date AS TheDate
,VPeakAvgCPUPct AS AvgCPUPct
,AVG(VPeakAvgCPUPct ) OVER (ORDER BY  Calendar_Date ROWS 21 PRECEDING) AS MovingAvg
,CASE WHEN VPeakAvgCPUPct IS NOT NULL THEN 1 ELSE 0 END AS CountX
,SUM(CountX) OVER ( ) AS CountAll
,ForecastX AS Trend
,80 AS ReserveX /* Enter the Reserve CPU percentage (in whole numbers).  Typically 80 - 90. */
,SlopeX
,PeakStart
,PeakEnd

FROM
(
SELECT
 SiteID
,Period_Number
,a4.Month_Of_Calendar
,a4.TheDate
,NULL (DECIMAL(38,6)) AS VPeakAvgCPUPct
,a4.TrendX
,a4.SlopeX
,NULL (CHAR(13)) as PeakStart
,NULL (CHAR(13)) as PeakEnd
,a4.TheHour
,c2.calendar_date
,COUNT(*) OVER (ORDER BY c2.calendar_date ROWS UNBOUNDED PRECEDING ) AS SequenceNbr
,a4.TrendX + (a4.SlopeX * SequenceNbr) AS ForecastX
,VPeakAvgCPUPct AS ExtVPeakAvgCPUPct

FROM
(SELECT
 SiteID
,Period_Number
,Month_Of_Calendar
,TheDate
,VPeakAvgCPUPct
,CAST(REGR_INTERCEPT(VPeakAvgCPUPct , Period_Number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS DECIMAL(30,6))
+ Period_Number * CAST((REGR_SLOPE(VPeakAvgCPUPct , Period_Number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING )) AS DECIMAL(30,6)) AS TrendX
,CAST(REGR_SLOPE(VPeakAvgCPUPct, Period_Number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS DECIMAL(30,6)) AS SlopeX
,PeakStart
,PeakEnd
,TheHour
FROM (
SELECT
 SiteID
,TheDate
,TheHour
,Month_Of_Calendar
,PeakStart
,PeakEnd
,VPeakAvgCPUPct
,ROW_NUMBER() OVER (ORDER BY TheDate) AS Period_Number
FROM (
SELECT
 SiteID
,TheDate
,TheHour
,Month_Of_Calendar
,PeakStart
,PeakEnd
,HourlyAvgCPUPct
,VPeakAvgCPUPct
FROM (
SELECT
 '{siteid}' as SiteID
,TheDate
,c1.Month_Of_Calendar
,(TheTime/10000 (SMALLINT)) AS TheHour
,( TheDate (DATE, FORMAT 'YYYY-MM-DD'))||' '||TRIM(TheHour (FORMAT '99')) AS PeakEnd
,AVG((((s1.CPUUServ (DECIMAL(38,6))) + s1.CPUUExec)/NULLIFZERO((s1.NCPUs (DECIMAL(38,6)))
))/(s1.Secs (DECIMAL(38,6)))
) AS HourlyAvgCPUPct
,AVG((HourlyAvgCPUPct (DECIMAL(38,6)))
) OVER (ORDER BY  TheDate ,TheHour ROWS 3 PRECEDING) AS VPeakAvgCPUPct /* Enter Peak Period duration (n-1).  Typically 4 hours = 3  */
,MIN((TheDate (DATE, FORMAT 'YYYY-MM-DD')) ||' '||TRIM(TheHour (FORMAT '99'))) OVER  (ORDER BY  TheDate ,TheHour ROWS 3 PRECEDING) AS PeakStart /* Enter Peak Period duration (n-1).  Typically 4 hours = 3  */
/* FROM DBC.ResUsageSPMA s1,
   FROM PDCRINFO.ResUsageSPMA_hst s1, */
FROM {resusagespma} s1,
sys_calendar.CALENDAR c1
WHERE  c1.calendar_date= s1.TheDate
AND c1.day_of_week IN (2,3,4,5,6)
AND s1.TheDate BETWEEN {startdate} AND {enddate}
GROUP BY 1,2,3,4) a1
QUALIFY ROW_NUMBER () OVER (PARTITION BY TheDate ORDER BY VPeakAvgCPUPct  DESC) = 1) a2
) a3
QUALIFY ROW_NUMBER () OVER (ORDER BY TheDate  DESC) = 1
) a4,
sys_calendar.CALENDAR c2
WHERE  c2.calendar_date BETWEEN a4.TheDate+1 AND a4.TheDate + (365*2) /* Enter number of days for future forecast.  Typically 365*2  */
AND c2.day_of_week IN (2,3,4,5,6)

UNION

SELECT
 SiteID
,Period_Number
,Month_Of_Calendar
,TheDate
,VPeakAvgCPUPct
,CAST(REGR_INTERCEPT(VPeakAvgCPUPct , Period_Number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS DECIMAL(30,6))
+ Period_Number * CAST((REGR_SLOPE(VPeakAvgCPUPct , Period_Number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING )) AS DECIMAL(30,6)) AS TrendX
,CAST(REGR_SLOPE(VPeakAvgCPUPct, Period_Number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS DECIMAL(30,6)) AS SlopeX
,PeakStart
,PeakEnd
,TheHour
,TheDate
,0
,TrendX
,VPeakAvgCPUPct AS ExtVPeakAvgCPUPct
FROM (
SELECT
 SiteID
,TheDate
,TheHour
,Month_Of_Calendar
,PeakStart
,PeakEnd
,VPeakAvgCPUPct
,ROW_NUMBER() OVER (ORDER BY TheDate) AS Period_Number
FROM (
SELECT
 SiteID
,TheDate
,TheHour
,Month_Of_Calendar
,PeakStart
,PeakEnd
,HourlyAvgCPUPct
,VPeakAvgCPUPct
FROM (
SELECT
'{siteid}' as SiteID
,TheDate
,c1.Month_Of_Calendar
,(TheTime/10000 (SMALLINT)) AS TheHour
,( TheDate (DATE, FORMAT 'YYYY-MM-DD'))||' '||TRIM(TheHour (FORMAT '99')) AS PeakEnd
,AVG((((s1.CPUUServ (DECIMAL(38,6))) + s1.CPUUExec)/NULLIFZERO((s1.NCPUs (DECIMAL(38,6)))
))/(s1.Secs (DECIMAL(38,6)))
) AS HourlyAvgCPUPct
,AVG((HourlyAvgCPUPct (DECIMAL(38,6)))
) OVER (ORDER BY  TheDate ,TheHour ROWS 3 PRECEDING) AS VPeakAvgCPUPct /* Enter Peak Period duration (n-1).  Typically 4 hours = 3  */
,MIN((TheDate (DATE, FORMAT 'YYYY-MM-DD')) ||' '||TRIM(TheHour (FORMAT '99'))) OVER  (ORDER BY  TheDate ,TheHour ROWS 3 PRECEDING) AS PeakStart /* Enter Peak Period duration (n-1).  Typically 4 hours = 3  */
/* FROM DBC.ResUsageSPMA s1,
   FROM PDCRINFO.ResUsageSPMA_hst s1, */
FROM {resusagespma} s1,
sys_calendar.CALENDAR c1
WHERE  c1.calendar_date= s1.TheDate
AND c1.day_of_week IN (2,3,4,5,6)
AND s1.TheDate BETWEEN {startdate} AND {enddate}
GROUP BY 1,2,3,4) a1
QUALIFY ROW_NUMBER () OVER (PARTITION BY TheDate ORDER BY VPeakAvgCPUPct  DESC) = 1) a2
) a3
) a4
WHERE ForecastX < 100
) a5
ORDER BY 1,2,3;
