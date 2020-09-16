/*
Query 7
###########################################
Query Output File Name: IO Trend Sldv 80Pct
Tableau Dashboard: Consumption CPU & I/O & Space Trend

I/O Utilization 4-Hour Variable Peak from ResusageSldv (Viewpoint I/O Utilization Method).
•	Evaluates the percentage of time in the collection period that devices were busy processing I/O requests (ldvOutReqTime) for ldvreads > 0 & ldvtype = 'DISK' (no need for Archie or I/O capacity estimates).
•	IOBusyPct assessment is at 80th percentile of busiest drives (not an average). 80% of devices are less busy, 20% of drives are more busy.  The premise is that when 20% of drives reach 80% busy (i.e., reserve capacity level) the system will likely suffer performance impact.
•	Evaluates 365 days of history (weekdays only) and the busiest 4-hours of each day (whenever they occur).  Peak periods are contiguous 4-hour periods (not individual hours) and may vary (PeakStart & PeakEnd displayed in result) depending on utilization.
•	Peak periods may start on one day and end in the next – peak periods are always recorded on the day in which they end.
•	Simple linear regression is used to determine trend line with slope & intercept.
•	The slope of the trend line is used to extend anticipated usage 365 days into the future or when utilization is forecasted to exceed 100% (whichever comes first).
•	21 day Moving Average is included to emphasize recent activity in addition to the longer trend (typically 21 business days in a calendar month).
•	Reserve Capacity is set at 80% (workload performance will likely be impacted when busiest 20% of drives exceed 80% utilization).
•	Reserve Horizon represents the (future) point in time at which utilization is expected to exceed 80%.
•	Slope is the daily percentage increase/decrease in utilization of the trend line (positive = increasing utilization, negative = decreasing utilization).
•	SQL uses UNION to combine historical trend with future forecast – identical changes typically must be made to both SQL statements in UNION.
##################################################################
*/

/*{{save:IO_Trend_Sldv_80Pct.csv}}*/
LOCK ROW FOR ACCESS
SELECT
 SiteID as SiteID  /* Enter the Customer SiteID */
,current_date (format'YYYY-MM-DD') (CHAR(10)) as "Report Date"
,TheDate (format'YYYY-MM-DD') (CHAR(10)) as "Log Date"
,PeakStart ||':00:00' as "Peak Start"
,PeakEnd ||':00:00' as "Peak End"
,AvgIOPct (DECIMAL(18,4)) as "Avg I/O Pct"
,CASE
 WHEN Period_Number < 21 THEN NULL
 WHEN AvgIOPct IS NULL THEN NULL
 ELSE MovingAvg END (DECIMAL(18,4)) AS "Moving Avg"
,Trend (DECIMAL(18,4)) as Trend
,ReserveX
,CASE WHEN Trend >= ReserveX THEN Trend ELSE NULL END (DECIMAL(18,4)) AS "Reserve Horizon"
,SlopeX (DECIMAL(18,4))as SlopeX

FROM
(
SELECT
--(a6)
 SiteID
,COUNT(*) OVER (ORDER BY calendar_date ROWS UNBOUNDED PRECEDING ) AS Period_Number
,Month_Of_Calendar
,Calendar_Date AS TheDate
,PeakStart
,PeakEnd
,VPeakAvgIOPct AS AvgIOPct
,AVG(VPeakAvgIOPct ) OVER (ORDER BY  Calendar_Date ROWS 21 PRECEDING) AS MovingAvg

,CASE WHEN VPeakAvgIOPct IS NOT NULL THEN 1 ELSE 0 END AS CountX
,SUM(CountX) OVER ( ) AS CountAll
,MIN(Trend*CountX) OVER ( ) AS MINTrend
,MAX(Trend*CountX) OVER ( ) AS MAXTrend
,ForecastX AS Trend
,80 AS ReserveX  /* Enter the Reserve I/O percentage (in whole numbers).  Typically 80 - 90. */
,MIN(Month_Of_Calendar) OVER () AS MinMonthAll
,MAX(Month_Of_Calendar) OVER () AS MAXMonthAll
,(MAXMonthAll - MINMOnthAll) + 1 AS MonthsNumber
,SlopeX

FROM (
--UNION-1 Forecast
SELECT
 a5.SiteID
,a5.Period_Number
,c2.Month_Of_Calendar
,a5.TheDate
,NULL (DECIMAL(38,6)) AS VPeakAvgIOPct
,a5.TrendX
,a5.SlopeX
,NULL (CHAR(13)) as PeakStart
,NULL (CHAR(13)) as PeakEnd
,a5.TheHour
,c2.calendar_date
,COUNT(*) OVER (ORDER BY c2.calendar_date ROWS UNBOUNDED PRECEDING ) AS SequenceNbr
,a5.TrendX + (a5.SlopeX * SequenceNbr) AS ForecastX
,VPeakAvgIOPct AS ExtVPeakAvgIOPct
,COUNT(*) OVER ( ) AS CountAll

FROM
(
--(a5)
SELECT
 SiteID
,Period_Number
,Month_Of_Calendar
,TheDate
,VPeakAvgIOPct
,CAST(REGR_INTERCEPT(VPeakAvgIOPct , Period_Number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS DECIMAL(30,6))
+ Period_Number * CAST((REGR_SLOPE(VPeakAvgIOPct , Period_Number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING )) AS DECIMAL(30,6)) AS TrendX
,CAST(REGR_SLOPE(VPeakAvgIOPct, Period_Number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS DECIMAL(30,6)) AS SlopeX
,PeakStart
,PeakEnd
,TheHour
FROM (
--(a4)
SELECT
 SiteID
,TheDate
,TheHour
,Month_Of_Calendar
,PeakStart
,PeakEnd
,VPeakAvgIOPct
,ROW_NUMBER() OVER (ORDER BY TheDate) AS Period_Number
FROM (
--(a3)
SELECT
 SiteID
,TheDate
,TheHour
,Month_Of_Calendar
,PeakStart
,PeakEnd
,HourlyAvgIOPct
,VPeakAvgIOPct
FROM (
--(a2)
SELECT
 SiteID
,TheDate
,Month_Of_Calendar
,TheHour
,( TheDate (DATE, FORMAT 'YYYY-MM-DD'))||' '||TRIM(TheHour (FORMAT '99')) AS PeakEnd
--,COUNT(*) AS LUNCount
,AVG(MinDiskPct) AS HourlyAvgIOPct
,AVG(HourlyAvgIOPct) OVER (ORDER BY  SiteID, TheDate ,TheHour ROWS 3 PRECEDING) AS VPeakAvgIOPct  /* Enter Peak Period duration (n-1).  Typically 4 hours = 3  */
,MIN((TheDate (DATE, FORMAT 'YYYY-MM-DD')) ||' '||TRIM(TheHour (FORMAT '99'))) OVER  (ORDER BY  SiteID, TheDate ,TheHour ROWS 3 PRECEDING) AS PeakStart  /* Enter Peak Period duration (n-1).  Typically 4 hours = 3  */
from
(
Select
--(CC) Identify 1st device with NumDiskPct >=0.80, eliminate all others
 'SiteID' as SiteID
,Month_Of_Calendar
,TheDate
,TheHour
,TheMinute
,MIN(DiskPct) as MinDiskPct
,AVG(DiskPct (DECIMAL(18,4))) as AvgDiskPct
,MAX(DiskPct) as MaxDiskPct
,MAX(TotalCount2) as TotalActiveDevices
,MIN(TotalCount3 ) - 1 as CountDevicesBelow80th
,Count(*) as CountDevicesAbove80th
,(CountDevicesBelow80th(DECIMAL(18,4)))/TotalActiveDevices as PctDevicesBelow80th
--,MIN(NumDiskPct) as PctDevicesBelow80th
,1-PctDevicesBelow80th as PctDevicesAbove80th
from (
--(BB) Reduce result to 20% most busy devices (i.e., 1st device with NumDiskPct >=0.80
Select
TheDate
,TheHour
,Month_Of_Calendar
,TheMinute
,AvgDiskPct2
,NodeID
,CtlID
,LdvID
,DiskPct
,Count(*) as TotalCount
,SUM(TotalCount) over (partition by TheDate, TheHour, TheMinute) as TotalCount2
,(SUM(TotalCount) over (partition by TheDate, TheHour, TheMinute order by TheDate, TheHour, TheMinute, DiskPct, NodeID, CtlID, LdvID
 ROWS UNBOUNDED PRECEDING))  as TotalCount3
,(TotalCount3 (DECIMAL(18,4)))/(TotalCount2 (DECIMAL(18,4))) as NumDiskPct
FROM (
--(AA) SELECT qualifying data from Sldv
--ldvreads > 0
--ldvtype = 'DISK'
select
 s1.TheDate
,c1.Month_Of_Calendar
,EXTRACT(HOUR from s1.TheTime) as TheHour
,EXTRACT(MINUTE from s1.TheTime) as TheMinute
,s1.NodeID
,s1.CtlID
,s1.LdvID
,(cast(s1.ldvOutReqTime as decimal(18,4))/secs) as DiskPct
,AVG(DiskPct) over (partition by TheDate, TheHour, TheMinute) as AvgDiskPct2
--from dbc.ResUsageSldv s1,
from PDCRINFO.ResUsageSldv_hst s1,
sys_calendar.CALENDAR c1
WHERE  c1.calendar_date= s1.TheDate
AND c1.day_of_week IN (2,3,4,5,6)
and s1.LdvType='DISK'
AND s1.ldvreads > 0
AND s1.TheDate BETWEEN {startdate_history} AND {enddate_history}  /* Enter number of days for history.  Typically 365  */
) as AA
Qualify NumDiskPct >= .80
group by TheDate, Month_Of_Calendar, TheHour, TheMinute, AvgDiskPct2, NodeID, CtlID, LdvID, DiskPct
-- Order by TheDate, TheHour, TheMinute, AvgDiskPct2, TotalCount3
) as BB
group by 1,2,3,4,5
) as CC
GROUP BY 1,2,3,4
) a2
QUALIFY ROW_NUMBER () OVER (PARTITION BY TheDate ORDER BY VPeakAvgIOPct  DESC) = 1
) a3
) a4
QUALIFY ROW_NUMBER () OVER (ORDER BY TheDate  DESC) = 1
) a5,
sys_calendar.CALENDAR c2
WHERE  c2.calendar_date BETWEEN a5.TheDate+1 AND a5.TheDate + (365*2)  /* Enter number of days for future forecast.  Typically 365*2  */
AND c2.day_of_week IN (2,3,4,5,6)

UNION

SELECT
--UNION-2 Historical Trend Line
 SiteID
,Period_Number
,Month_Of_Calendar
,TheDate
,VPeakAvgIOPct
,CAST(REGR_INTERCEPT(VPeakAvgIOPct , Period_Number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS DECIMAL(30,6))
+ Period_Number * CAST((REGR_SLOPE(VPeakAvgIOPct , Period_Number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING )) AS DECIMAL(30,6)) AS TrendX
,CAST(REGR_SLOPE(VPeakAvgIOPct, Period_Number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS DECIMAL(30,6)) AS SlopeX
,PeakStart
,PeakEnd
,TheHour
,TheDate
,0 as SequenceNbr
,TrendX AS ForecastX
,VPeakAvgIOPct AS ExtVPeakAvgIOPct
,COUNT(*) OVER ( ) AS CountAll
FROM (
SELECT
 SiteID
,TheDate
,TheHour
,Month_Of_Calendar
,PeakStart
,PeakEnd
,VPeakAvgIOPct
,ROW_NUMBER() OVER (ORDER BY TheDate) AS Period_Number
FROM (
SELECT
 SiteID
,TheDate
,TheHour
,Month_Of_Calendar
,PeakStart
,PeakEnd
,HourlyAvgIOPct
,VPeakAvgIOPct
FROM (
SELECT
 SiteID
,TheDate
,Month_Of_Calendar
,TheHour
,( TheDate (DATE, FORMAT 'YYYY-MM-DD'))||' '||TRIM(TheHour (FORMAT '99')) AS PeakEnd
--,COUNT(*) AS LUNCount
,AVG(MinDiskPct) AS HourlyAvgIOPct
,AVG(HourlyAvgIOPct) OVER (ORDER BY  SiteID, TheDate ,TheHour ROWS 3 PRECEDING) AS VPeakAvgIOPct /* Enter Peak Period duration (n-1).  Typically 4 hours = 3  */
,MIN((TheDate (DATE, FORMAT 'YYYY-MM-DD')) ||' '||TRIM(TheHour (FORMAT '99'))) OVER  (ORDER BY  SiteID, TheDate ,TheHour ROWS 3 PRECEDING) AS PeakStart /* Enter Peak Period duration (n-1).  Typically 4 hours = 3  */
from
(
Select
--(CC) Identify 1st device with NumDiskPct >=0.80, eliminate all others
 'SiteID' as SiteID
,Month_Of_Calendar
,TheDate
,TheHour
,TheMinute
,MIN(DiskPct) as MinDiskPct
,AVG(DiskPct (DECIMAL(18,4))) as AvgDiskPct
,MAX(DiskPct) as MaxDiskPct
,MAX(TotalCount2) as TotalActiveDevices
,MIN(TotalCount3 ) - 1 as CountDevicesBelow80th
,Count(*) as CountDevicesAbove80th
,(CountDevicesBelow80th(DECIMAL(18,4)))/TotalActiveDevices as PctDevicesBelow80th
--,MIN(NumDiskPct) as PctDevicesBelow80th
,1-PctDevicesBelow80th as PctDevicesAbove80th
from (
--(BB) Reduce result to 20% most busy devices (i.e., 1st device with NumDiskPct >=0.80
Select
TheDate
,TheHour
,Month_Of_Calendar
,TheMinute
,AvgDiskPct2
,NodeID
,CtlID
,LdvID
,DiskPct
,Count(*) as TotalCount
,SUM(TotalCount) over (partition by TheDate, TheHour, TheMinute) as TotalCount2
,(SUM(TotalCount) over (partition by TheDate, TheHour, TheMinute order by TheDate, TheHour, TheMinute, DiskPct, NodeID, CtlID, LdvID
 ROWS UNBOUNDED PRECEDING))  as TotalCount3
,(TotalCount3 (DECIMAL(18,4)))/(TotalCount2 (DECIMAL(18,4))) as NumDiskPct
FROM (
--(AA) SELECT qualifying data from Sldv
--ldvreads > 0
--ldvtype = 'DISK'
select
 s1.TheDate
,c1.Month_Of_Calendar
,EXTRACT(HOUR from s1.TheTime) as TheHour
,EXTRACT(MINUTE from s1.TheTime) as TheMinute
,s1.NodeID
,s1.CtlID
,s1.LdvID
,(cast(s1.ldvOutReqTime as decimal(18,4))/secs) as DiskPct
,AVG(DiskPct) over (partition by TheDate, TheHour, TheMinute) as AvgDiskPct2
--from dbc.ResUsageSldv s1,
from PDCRINFO.ResUsageSldv_hst s1,
sys_calendar.CALENDAR c1
WHERE  c1.calendar_date= s1.TheDate
AND c1.day_of_week IN (2,3,4,5,6)
and s1.LdvType='DISK'
AND s1.ldvreads > 0
AND s1.TheDate BETWEEN {startdate_history} AND {enddate_history}  /* Enter number of days for history.  Typically 365  */
) as AA
Qualify NumDiskPct >= .80
group by TheDate, Month_Of_Calendar, TheHour, TheMinute, AvgDiskPct2, NodeID, CtlID, LdvID, DiskPct
--Order by TheDate, TheHour, TheMinute, AvgDiskPct2, TotalCount3
) as BB
group by 1,2,3,4,5
) as CC
GROUP BY 1,2,3,4
) a2
QUALIFY ROW_NUMBER () OVER (PARTITION BY TheDate ORDER BY VPeakAvgIOPct  DESC) = 1
) a3
) a4
) a6
WHERE ForecastX < 100  /* Enter percentage of forecast capacity (in whole numbers).  Typically 100  */
) a7
--ORDER BY 1,2,3
;
