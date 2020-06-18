/*
Query 11
###########################################
CPU and IO heatmap

Query Output File Name: IO Utilization Detail from ResusageSldv
Tableau Dashboard: I/O Daily Avg & Trend
*/

/*{{save:IO_Utilization_Detail_from_ResusageSldv.csv}}*/
LOCK ROW FOR ACCESS
Select
--(XX) Identify 1st device with NumDiskPct >=0.80, eliminate all others
'TDCLOUD14TD07' (VARCHAR(30)) as SiteID --< enter customer SiteID here
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
where thedate BETWEEN {startdate_history} and {enddate_history}
and ldvreads > 0
and LdvType='DISK'
) as AA
Qualify NumDiskPct >= .80
group by TheDate, TheHour, TheMinute, TheTime, AvgDiskPct2, NodeID, CtlID, LdvID, DiskPct
) as BB
group by 1,2,3,4,5,6
-- order by 1,2,3,4,5
;
