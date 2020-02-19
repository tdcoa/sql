
/*----- Description ----- ----- ----- ----- -----

I/O Detail Sldv 80Pct
DBC Option (uses dbc.ResusageSldv)
Version 11 (2019-05-07)
Current-365 days history

I/O Utilization Detail from ResusageSldv (Viewpoint method)
•	Evaluates the percentage of time in the collection period that devices were busy processing I/O requests (ldvOutReqTime) for ldvreads > 0 & ldvtype = 'DISK' (no need for Archie or I/O capacity estimates).
•	IOBusyPct assessment is at 80th percentile of busiest drives (not an average). 80% of devices are less busy, 20% of drives are more busy.  The premise is that when 20% of drives reach 80% busy (i.e., reserve capacity level) the system will likely suffer performance impact.
•	Extracts 365 days of history at the default collection rate (typically 10-minute intervals).
•	Data is unfiltered by peak-period or date/hour and is at the lowest level of detail typically needed for various trending options (daily average, fixed peak, variable peak, etc.).
•	Requires logging on DBC.ResusageSldv, preferably at 10-minute collection periods (600 seconds).

Execution Instructions
•	Copy/Paste below query into favorite query tool & execute.
•	Copy/Paste result set from query tool into Excel and save as .xls or .xlsx
o	Don’t export results (impacts formatting).
o	Don’t save results to spreadsheeting (impacts formatting).
•	Use visualization tool to import results for analysis (coming soon).


Configurable parameters
Changes can be made by modifying the SQL below (change the yellow highlighted values):
(1) Change the literal ‘SiteID’ to the actual SiteID for the customer system.
'SiteID' as SiteID
(2) Historical date range two changes required.
where thedate BETWEEN (Current_Date - 365) and Current_Date

----- SQL ----- ----- ----- ----- -----*/
--insert into adlste_westcomm.Consumption_IO_Detail_Sldv_80pct_V3
LOCK ROW FOR ACCESS
Select
--(XX) Identify 1st device with NumDiskPct >=0.80, eliminate all others
'TDCLOUD14TD07' (VARCHAR(30)) as SiteID --<-- enter customer SiteID here
,TheDate (FORMAT 'YYYY-MM-DD') as "Log Date"
,TheHour as "Log Hour"
,TheMinute as "Log Minute"
,TheTime as "Log Time"
,cast(cast((thedate(format'YYYY-MM-DD'))||' '||cast(thetime as char(2))||':'||cast(((extract(minute from TheTime))/10 (format'9')) as char(1))||'0:00' as timestamp(0)) as CHAR(25)) as "Log TimeStamp"
,MIN(DiskPct) as "Min Disk Pct"
,AVG(DiskPct (DECIMAL(18,4))) as "Avg Disk Pct"
,MAX(DiskPct (DECIMAL(18,4))) as "Max Disk Pct"
,MAX(TotalCount2) (INTEGER) as "Total Active Devices"
,MIN(TotalCount3) (INTEGER) as "Count Devices Below 80th"
,Count(*) (INTEGER) as "Count Devices Above 80th"
,MIN(NumDiskPct) (DECIMAL(18,4))  as "Pct Devices Below 80th"
,(1-"Pct Devices Below 80th") (DECIMAL(18,4))  as "Pct Devices Above 80th"
from (
--(BB) Reduce result to 20% most busy devices (i.e., 1st device with NumDiskPct >=0.80
Select
 TheDate
,TheHour
,TheMinute
,TheTime
,AvgDiskPct2
,NodeID
,CtlID
,LdvID
,DiskPct
,Count(*) as TotalCount
,SUM(TotalCount) over (partition by TheDate, TheHour, TheMinute) as TotalCount2
,SUM(TotalCount) over (partition by TheDate, TheHour, TheMinute order by TheDate, TheHour, TheMinute, DiskPct, NodeID, CtlID, LdvID
 ROWS UNBOUNDED PRECEDING) as TotalCount3
,(TotalCount3 (DECIMAL(18,4)))/(TotalCount2 (DECIMAL(18,4))) as NumDiskPct
FROM (
--(AA) SELECT qualifying data from Sldv
select
 TheDate
,EXTRACT(HOUR from TheTime) as TheHour
,EXTRACT(MINUTE from TheTime) as TheMinute
,TheTime
,NodeID
,CtlID
,LdvID
,(cast(ldvOutReqTime as decimal(18,4))/secs) as DiskPct
,AVG(DiskPct) over (partition by TheDate, TheHour, TheMinute) as AvgDiskPct2
from PDCRINFO.ResUsageSldv_hst
--from PDCRINFO.ResUsageSldv_hst
where thedate BETWEEN (Current_Date - 365) and Current_Date
and ldvreads > 0
and LdvType='DISK'
) as AA
Qualify NumDiskPct >= .80
group by TheDate, TheHour, TheMinute, TheTime, AvgDiskPct2, NodeID, CtlID, LdvID, DiskPct
) as BB
group by 1,2,3,4,5,6
order by 1,2,3,4,5
;
