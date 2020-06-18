/*
Query 9
###########################################
CPU and IO heatmap

Query Output File Name: SPMA-Detail-Data
Tableau Dashboard: CPU Heatmap of Avg % Util  - SPMA Detail


*/

/*{{save:SPMA-Detail-Data.csv}}*/
SELECT
'SiteID' --< enter customer SiteID here
,TheDate (FORMAT 'YYYY-MM-DD') AS "Log Date"
,Extract(Hour from TheTime) AS "Log Hour"
,Extract(Minute from TheTime) AS "Log Minute"
,TheTime AS "Log Time"
,cast((thedate(format'YYYY-MM-DD'))||' '||cast(thetime as char(2))||':'||cast(((extract(minute from TheTime))(format'99')) as char(2))||':00' as timestamp(0)) as "Sys Time"
,COUNT(distinct NodeID) NodeCount
,SUM(s1.Secs) SecondCount
,AVG((((s1.CPUUServ (DECIMAL(38,6))) + s1.CPUUExec)/NULLIFZERO((s1.NCPUs (DECIMAL(38,6)))))/(s1.Secs (DECIMAL(38,6)))) AS AvgCPUPct
,SUM(FilePreReadKB) as "File Pre Read KB"
,SUM(FileAcqReadKB) as "File Acq Read KB"
,SUM(FileWriteKB)   as "File Write KB"
FROM PDCRINFO.ResUsageSpma_Hst s1
where TheDate between ({startdate_history}) AND {enddate_history}
group by 1,2,3,4,5,6
-- order by 1,2,3,4,5,6
;
