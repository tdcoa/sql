/*
##############################################
Query 19
	
Query Output File Name: CPUByOperatingWindow
Tableau Dashboard: CPU Consumption By Operating Window
*/

/*{{save:CPUByOperatingWindow.csv}}*/
    SELECT
	cast(QryLog.starttime as date) AS "Log Date"
	,EXTRACT( HOUR FROM QryLog.starttime) AS "Log Hour"
	,username As UserName  
  --,(EXTRACT (MINUTE FROM QryLog.starttime)/10)*10 AS Log10Minute 	
	,CASE
       WHEN  c.Day_of_week  IN (1,7) THEN 'WE'
       WHEN  c.Day_of_week  IN (2,3,4,5,6) AND  "Log Hour"  between 8 and 18 THEN 'BUS'
       WHEN  c.Day_of_week  IN (2,3,4,5,6) AND  "Log Hour" IN (19,20,21,22,23,0,1,2,3)THEN 'BAT-OP'
       ELSE 'BAT-P'
       END OperatingWindow 
	
	--,UPPER(U.Department) as Department
	--,UPPER(U.SubDepartment) as BusinessGroup
	,SUM(QryLog.AMPCPUTime + QryLog.ParserCPUTime) (BIGINT) as SUMCPUTime
	,SUM(QryLog.TotalIOCount) (BIGINT) as TotalIOCount
	,COUNT(*) as QueryCount
	FROM dbc.DBQLogTbl QryLog
	INNER JOIN Sys_Calendar.CALENDAR  c  ON cast(QryLog.starttime as date) = c.Calendar_date
	--INNER JOIN systemfe.ca_user_xref U
	--	ON QryLog.UserName = U.UserName
	WHERE cast(QryLog.starttime as date) BETWEEN date -2 AND date -1
	and c.Calendar_date BETWEEN date -2 AND date -1
	GROUP BY 1,2,3,4;
