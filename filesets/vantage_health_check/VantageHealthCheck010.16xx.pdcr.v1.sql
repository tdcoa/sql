/*
Query 10
###########################################
CPU and IO heatmap

Query Output File Name: SPMA-Detail-Data
Tableau Dashboard: CPU Heatmap of Avg % Util  - SPMA Detail
*/

/*{{save:SPMA-Detail-Data}}*/
SELECT
'SiteID'  (VARCHAR(30)) as SiteID --< enter customer SiteID here
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
where TheDate between ({startdate_history}) AND {enddate_history}
group by 1,2,3,4,5,6, 8,9,10,11,12,13
--order by 1,2,3,4,5
;
