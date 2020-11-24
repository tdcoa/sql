LOCKING	ROW FOR ACCESS 
SELECT	logdate, cpu_utilize, cpubusy cpu_count 
FROM(
SELECT	logdate(FORMAT 'yyyy-mm-dd') logdate, (CPUBUSY / NULLIFZERO(CPUTotal)) * 100 AS cpu_utilize ,
		SUM(CPUServSec + CPUExecSec + CPUWaitIOSec + CPUIdleSec) AS CPUTotal,
		SUM(CPUServSec + CPUExecSec)  AS CPUBusy 
FROM	PDCRINFO.ResUsageSum10_hst a 
WHERE	logdate >= DATE - 90 
GROUP BY logdate) a;
