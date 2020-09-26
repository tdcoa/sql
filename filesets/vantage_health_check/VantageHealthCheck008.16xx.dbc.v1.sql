/*   
Query 8  

###########################################
Query Output File Name: Storage Trend DatabaseSpace AvgCurrentPermPct
Tableau Dashboard: Consumption CPU & I/O & Space Trend


Storage Trend DatabaseSpace AvgCurrentPermPct

Storage Trend & Forecast  
•	Simple linear regression is used to determine trend line with slope & intercept.
•	The slope of the trend line is used to extend anticipated usage 365 days into the future or when utilization is forecasted to exceed 100% (whichever comes first). 
•	21 day Moving Average is included to emphasize recent activity in addition to the longer trend (typically 21 business days in a calendar month).
•	Reserve Capacity is set at 80% (workload performance will likely be impacted when CPU’s exceed 80% utilization).
•	Reserve Horizon represents the (future) point in time at which utilization is expected to exceed 80%.
•	Slope is the daily percentage increase/decrease in utilization of the trend line (positive = increasing utilization, negative = decreasing utilization). 
•	SQL uses UNION to combine historical trend with future forecast – identical changes typically must be made to both SQL statements in UNION.
*/

/*{{save:Storage_Trend_DatabaseSpace_AvgCurrentPermPct.csv}}*/
LOCK ROW FOR ACCESS 
SELECT
 'SiteID'  /* Enter the Customer SiteID */
,Current_Date (format'YYYY-MM-DD') (CHAR(10)) as "Report Date"
,TheDate(format'YYYY-MM-DD') (CHAR(10)) as "Log Date"
,TotalMaxPerm as "Total Max Perm"
,TotalCurPerm as "Total Current Perm"
,TotalPeakPerm as "Total Peak Perm"
,TotalAvailPerm as "Total Available Perm"
,TotalCurPct (DECIMAL(18,4)) as "Total Current Pct"
,TotalAvailPct (DECIMAL(18,4)) as "Total Available Pct" 
,CASE 
 WHEN Period_Number < 21 THEN NULL 
 WHEN TotalCurPct IS NULL THEN NULL 
 ELSE MovingAvg END (DECIMAL(18,4)) AS "Moving Avg"
,Trend (DECIMAL(18,4)) as Trend
,ReserveX (DECIMAL(18,4)) as ReserveX
,CASE WHEN Trend >= ReserveX THEN Trend ELSE NULL END (DECIMAL(18,4)) AS "Reserve Horizon" 
,SlopeX (DECIMAL(18,4)) as SlopeX

FROM
(
SELECT
 SiteID
,COUNT(*) OVER (ORDER BY calendar_date ROWS UNBOUNDED PRECEDING ) AS Period_Number
,Calendar_Date AS TheDate
,TotalMaxPerm
,TotalCurPerm
,TotalPeakPerm
,TotalAvailPerm
,TotalCurPct
,TotalAvailPct
,AVG(TotalCurPct) OVER (ORDER BY  Calendar_Date ROWS 21 PRECEDING) AS MovingAvg
,TrendX
,SlopeX
,70 AS ReserveX /* Enter the amount of the storage reserve threshold – typically 65 to 80 */
,ForecastX
,ForecastX AS Trend
FROM 
(
SELECT
   SiteID
  ,c2.calendar_date
  ,NULL (BIGINT) as TotalMaxPerm
  ,NULL (BIGINT) as TotalCurPerm
  ,NULL (BIGINT) as TotalPeakPerm
  ,NULL (BIGINT) as TotalAvailPerm
  ,NULL (DECIMAL(18,4)) as TotalCurPct
  ,NULL (DECIMAL(18,4)) as TotalAvailPct
  ,COUNT(*) OVER (ORDER BY c2.calendar_date ROWS UNBOUNDED PRECEDING ) AS SequenceNbr
  ,TrendX
  ,SlopeX
  ,TrendX + (SlopeX * SequenceNbr) AS ForecastX

FROM 
(
SELECT
   SiteID
  ,LogDate
  ,TotalMaxPerm
  ,TotalCurPerm
  ,TotalPeakPerm
  ,TotalAvailPerm
  ,TotalCurPct
  ,TotalAvailPct
  ,Period_Number
  ,CAST(REGR_INTERCEPT(TotalCurPct , Period_Number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS DECIMAL(30,8))
+ Period_Number * CAST((REGR_SLOPE(TotalCurPct , Period_Number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING )) AS DECIMAL(30,8)) AS TrendX
,CAST(REGR_SLOPE(TotalCurPct, Period_Number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS DECIMAL(30,8)) AS SlopeX
FROM (
SELECT
   SiteID
  ,LogDate
  ,TotalMaxPerm
  ,TotalCurPerm
  ,TotalPeakPerm
  ,TotalAvailPerm
  ,TotalCurPct
  ,TotalAvailPct
  ,ROW_NUMBER() OVER (ORDER BY LogDate) AS Period_Number
FROM (
SELECT
  'SiteID' as SiteID
  ,LogDate
  ,SUM(MAXPERM)                      AS TotalMaxPerm
  ,SUM(CURRENTPERM)                  AS TotalCurPerm
  ,SUM(PEAKPERM)                     AS TotalPeakPerm
  ,TotalMaxPerm-TotalCurPerm         AS TotalAvailPerm
  ,TotalCurPerm/(TotalMaxPerm (DECIMAL(38,4))) * 100   AS TotalCurPct
  ,TotalAvailPerm/(TotalMaxPerm (DECIMAL(38,4)))* 100 AS TotalAvailPct
 
  FROM  DBC.DatabaseSpace s1,
      sys_calendar.CALENDAR c1
  WHERE  c1.calendar_date= s1.LogDate
    AND c1.day_of_week IN (2,3,4,5,6) 
    AND s1.Logdate BETWEEN {startdate_history} AND {enddate_history} /* Enter the number days history */
  Group by 1,2 
) a1
) a2
QUALIFY ROW_NUMBER () OVER (ORDER BY LogDate  DESC) = 1
) a3,
sys_calendar.CALENDAR c2
WHERE  c2.calendar_date BETWEEN a3.LogDate+1 AND (a3.LogDate + (365*2)) /* Enter the number days future forecast */
AND c2.day_of_week IN (2,3,4,5,6)

UNION

SELECT
   SiteID
  ,LogDate
  ,TotalMaxPerm
  ,TotalCurPerm
  ,TotalPeakPerm
  ,TotalAvailPerm
  ,TotalCurPct
  ,TotalAvailPct
  ,Period_Number
  ,CAST(REGR_INTERCEPT(TotalCurPct , Period_Number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS DECIMAL(30,8))
   + Period_Number * CAST((REGR_SLOPE(TotalCurPct , Period_Number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING )) AS DECIMAL(30,8)) AS TrendX
  ,CAST(REGR_SLOPE(TotalCurPct, Period_Number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS DECIMAL(30,8)) AS SlopeX
  ,TrendX as ForecastX 
FROM (
SELECT
   SiteID
  ,LogDate
  ,TotalMaxPerm
  ,TotalCurPerm
  ,TotalPeakPerm
  ,TotalAvailPerm
  ,TotalCurPct
  ,TotalAvailPct
  ,ROW_NUMBER() OVER (ORDER BY LogDate) AS Period_Number
FROM (
SELECT
  'SiteID' as SiteID 
  ,LogDate
  ,SUM(MAXPERM)                      AS TotalMaxPerm
  ,SUM(CURRENTPERM)                  AS TotalCurPerm
  ,SUM(PEAKPERM)                     AS TotalPeakPerm
  ,TotalMaxPerm-TotalCurPerm         AS TotalAvailPerm
  ,TotalCurPerm/(TotalMaxPerm (DECIMAL(38,4))) * 100   AS TotalCurPct
  ,TotalAvailPerm/(TotalMaxPerm (DECIMAL(38,4)))* 100 AS TotalAvailPct
 
  FROM  DBC.DatabaseSpace s1,
      sys_calendar.CALENDAR c1
  WHERE  c1.calendar_date= s1.LogDate
    AND c1.day_of_week IN (2,3,4,5,6) 
    AND s1.Logdate BETWEEN {startdate_history} AND {enddate_history} /* Enter the number days history */
  Group by 1,2 
) a1
) a2
) a3
) a4
WHERE ForecastX < 100 /* Enter the percentage of storage capacity (whole number).  Typically 100.  */
--order by 1,2,3
;

