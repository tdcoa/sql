
/*----- Description ----- ----- ----- ----- -----

CPU Detail Sldv 80Pct
DBC Option (uses dbc.ResusageSpma)
Version 11 (2019-05-07)
Current-365 days history

CPU Utilization Detail from ResusageSpma (Viewpoint method)
•	Average CPU Busy percent based on CPUUServ + CPUUExec
•	Extracts 365 days of history at the default collection rate (typically 10-minute intervals).
•	Data is unfiltered by peak-period or date/hour and is at the lowest level of detail typically needed for various trending options (daily average, fixed peak, variable peak, etc.).
•	Requires logging on DBC.ResusageSpma, preferably at 10-minute collection periods (600 seconds).

Execution Instructions
•	Copy/Paste below query into favorite query tool & execute.
•	Copy/Paste result set from query tool into Excel and save as .xls or .xlsx
o	Don’t export results (impacts formatting).
o	Don’t save results to spreadsheet (impacts formatting).
•	Use visualization tool to import results for analysis (coming soon).


Configurable parameters
Changes can be made by modifying the SQL below (change the yellow highlighted values):
(1) Change the literal ‘SiteID’ to the actual SiteID for the customer system.
'SiteID' as SiteID
(2) Historical date range two changes required.
where thedate BETWEEN (Current_Date - 365) and Current_Date

----- SQL ----- ----- ----- ----- -----*/
--insert into adlste_westcomm.Consumption_CPU_Detail_Spma_V3 
SELECT
'TDCLOUD14TD07'  (VARCHAR(30)) as SiteID --<-- enter customer SiteID here
,TheDate (FORMAT 'YYYY-MM-DD') as "Log Date"
,Extract(Hour from TheTime) AS "Log Hour"
,Extract(Minute from TheTime) AS "Log Minute"
,TheTime as "Log Time"
,cast(cast((thedate(format'YYYY-MM-DD'))||' '||cast(thetime as char(2))||':'||cast(((extract(minute from TheTime))/10 (format'9')) as char(1))||'0:00' as timestamp(0)) as CHAR(25)) as "Log TimeStamp"
,MAX(NodeType) as "Node Type"
,PM_COD_CPU as "PM COD CPU"
,PM_COD_IO as "PM COD IO"
,WM_COD_CPU as "WM COD CPU"
,WM_COD_IO as "WM COD IO"
,TIER_FACTOR as "Tier Factor"
,NCPUs as "Number CPUs"
,COUNT(distinct NodeID) as "Node Count"
,SUM(s1.Secs)/"Node Count" as "Collection Period Seconds"
,AVG((((s1.CPUUServ (DECIMAL(38,6))) + s1.CPUUExec)/NULLIFZERO((s1.NCPUs (DECIMAL(38,6)))))/(s1.Secs (DECIMAL(38,6)))) AS "Avg CPU Pct"
,SUM(FilePreReads) as "File Pre-Reads"
,SUM(FileAcqReads) as "File Acq-Reads"
,SUM(FileWrites) as "File Writes"
,SUM(FilePreReadKB) as "File Pre-Read KB"
,SUM(FileAcqReadKB) as "File Acq-Read KB"
,SUM(FileWriteKB)   as "File Write KB"
FROM PDCRINFO.ResUsageSPMA_hst s1
where TheDate between (Current_Date - 365) AND Current_Date
group by 1,2,3,4,5,6, 8,9,10,11,12,13
--order by 1,2,3,4,5
;
