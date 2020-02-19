
/*----- Description ----- ----- ----- ----- -----

Storage Trend DatabaseSpace AvgCurrentPermPct
PDCR Option (uses PDCRINFO.DatabaseSpace_Hst)
Version 11 (2019-05-07)
Current-365 days history & Current+365 days forecast

CPU Utilization 4-Hour Variable Peak from ResusageSpma (Viewpoint CPU Utilization Method).
•	Evaluates the percentage of time CPU’s in the collection period that CPU’s were busy processing requests (CPUUServ + CPUUExec ??CPUNice??
•	AvgCPUBusyPct is an average of CPU utilization across the entire system. The premise is that when CPU’s reach 80% busy (i.e., reserve capacity level) the system will likely suffer performance impact.
•	Evaluates 120 days of history (weekdays only) and the busiest 4-hours of each day (whenever they occur).  Peak periods are contiguous 4-hour periods (not individual hours) and may vary (PeakStart & PeakEnd displayed in result) depending on utilization.
•	Peak periods may start on one day and end in the next – peak periods are always recorded on the day in which they end.
•	Simple linear regression is used to determine trend line with slope & intercept.
•	The slope of the trend line is used to extend anticipated usage 365 days into the future or when utilization is forecasted to exceed 100% (whichever comes first).
•	21 day Moving Average is included to emphasize recent activity in addition to the longer trend (typically 21 business days in a calendar month).
•	Reserve Capacity is set at 80% (workload performance will likely be impacted when CPU’s exceed 80% utilization).
•	Reserve Horizon represents the (future) point in time at which utilization is expected to exceed 80%.
•	Slope is the daily percentage increase/decrease in utilization of the trend line (positive = increasing utilization, negative = decreasing utilization).
•	SQL uses UNION to combine historical trend with future forecast – identical changes typically must be made to both SQL statements in UNION.

Execution Instructions
•	Copy/Paste below query into favorite query tool & execute.
•	Copy/Paste result set from query tool into Excel and save as .xls or .xlsx
o	Don’t export results (impacts formatting).
o	Don’t save results to spreadsheeting (impacts formatting).
•	Use visualization tool to import results for analysis (coming soon).

Configurable parameters
Changes can be made by modifying the SQL below (change the yellow highlighted values):
(1) Change the literal ‘SiteID’ to the actual SiteID for the customer system (two changes required).
'SiteID' as SiteID
(2) Reserve Capacity – default is 70% for spool reserve (only one change required)
,70 AS ReserveX
(3) Historical number of days evaluated for trend line – two changes required
       AND s1.Logdate BETWEEN (Current_Date - 365) AND Current_Date
(4) Future forecast - number of days into the future for extended trend line – two changes required.
WHERE  c2.calendar_date BETWEEN a3.LogDate+1 AND (a3.LogDate + 365)

----- SQL ----- ----- ----- ----- -----*/
LOCK ROW FOR ACCESS
SELECT
  'SiteID' as SiteID
  ,LogDate as "Log Date"
  ,SUM(MAXPERM)                      AS "Total MaxPerm"
  ,SUM(CURRENTPERM)                  AS "Total CurrentPerm"
  ,SUM(PEAKPERM)                     AS "Total PeakPerm"
  ,"Total MaxPerm" - "Total CurrentPerm"         AS "Total Available Perm"
  ,"Total CurrentPerm"/("Total MaxPerm" (DECIMAL(38,4))) * 100   AS "Total Current Pct"
  ,"Total Available Perm"/("Total MaxPerm" (DECIMAL(38,4)))* 100 AS "Total Available Pct"
  --FROM  ss160000.DatabaseSpace s1
  FROM PDCRINFO.DatabaseSpace_Hst s1
  WHERE s1.Logdate BETWEEN (Current_Date - 365) AND Current_Date
  Group by 1,2
  Order by 1,2;
