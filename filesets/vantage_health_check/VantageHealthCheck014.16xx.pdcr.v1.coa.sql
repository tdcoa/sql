/*
##############################################3
Query 14
Query Output File Name: ResponseTimeBucket
Tableau Dashboard: QueryRunTimes
*/

/*{{save:ResponseTimeBucket.csv}}*/
Select
ResponseT.logdate "Log Date"
,ResponseT.username "User"
,Sum(ResponseT.QryCount) QryCount
,SUM(ResponseT."RT < 1 sec") "RT < 1 sec"
,SUM(ResponseT."RT 1-5 sec") "RT 1-5 sec"
,SUM(ResponseT."RT 5-10 sec") "RT 5-10 sec"
,SUM(ResponseT."RT 10-30 sec") "RT 10-30 sec"
,SUM(ResponseT."RT 30-60 sec") "RT 30-60 sec"
,SUM(ResponseT."RT 1-5 min")"RT 1-5 min"
,SUM(ResponseT."RT 5-10 min")"RT 5-10 min"
,SUM(ResponseT."RT 10-30 min")"RT 10-30 min"
,SUM(ResponseT."RT 30-60 min")"RT 30-60 min"
FROM(
Select logdate
,username
,((FirstRespTime - StartTime) HOUR(3) TO SECOND(6)) AS Execution_Time
,ZEROIFNULL(CAST(EXTRACT(HOUR FROM Execution_Time) * 3600 + EXTRACT(MINUTE FROM Execution_Time) * 60 + EXTRACT(SECOND FROM Execution_Time) AS FLOAT)) AS Execution_Time_Secs

,CASE
                WHEN Execution_Time_Secs IS NULL
                    THEN '00000 - 000000'
				WHEN Execution_Time_Secs < 1.0
                    THEN '00000 - 000001'
                WHEN Execution_Time_Secs BETWEEN 1.0 AND 1e1
                    THEN '00001 - 000010'
                WHEN Execution_Time_Secs BETWEEN 1e1 AND 1e2
                    THEN '00010 - 000100'
                WHEN Execution_Time_Secs BETWEEN 1e2 AND 1e3
                    THEN '00100 - 001000'
                WHEN Execution_Time_Secs BETWEEN 1e3 AND 1e4
                    THEN '01000 - 010000'
                WHEN Execution_Time_Secs > 1e4
                    THEN '10000+'
            END AS Execution_Time_Class
,Count(*) QryCount
,SUM(CASE WHEN 	Execution_Time_Secs < 1.0 THEN Execution_Time_Secs END) AS "RT < 1 sec"
,SUM(CASE WHEN 	Execution_Time_Secs BETWEEN 1.0 AND 5.0 THEN Execution_Time_Secs END) AS "RT 1-5 sec"
,SUM(CASE WHEN 	Execution_Time_Secs BETWEEN 5.0 AND 10.0 THEN Execution_Time_Secs END) AS "RT 5-10 sec"
,SUM(CASE WHEN 	Execution_Time_Secs BETWEEN 10.0 AND 30.0 THEN Execution_Time_Secs END) AS "RT 10-30 sec"
,SUM(CASE WHEN 	Execution_Time_Secs BETWEEN 30.0 AND 60.0 THEN Execution_Time_Secs END) AS "RT 30-60 sec"
,SUM(CASE WHEN 	Execution_Time_Secs BETWEEN 60.0 AND 300.0 THEN Execution_Time_Secs END) AS "RT 1-5 min"
,SUM(CASE WHEN 	Execution_Time_Secs BETWEEN 300.0 AND 600.0 THEN Execution_Time_Secs END) AS "RT 5-10 min"
,SUM(CASE WHEN 	Execution_Time_Secs BETWEEN 600.0 AND 1800.0 THEN Execution_Time_Secs END) AS "RT 10-30 min"
,SUM(CASE WHEN 	Execution_Time_Secs BETWEEN 1800.0 AND 3600.0 THEN Execution_Time_Secs END) AS "RT 30-60 min"
,SUM(CASE WHEN 	Execution_Time_Secs > 3600.0 THEN Execution_Time_Secs END) AS "RT > 1 hour"
FROM PDCRINFO.DBQLogTbl_Hst QryLog
            INNER JOIN
            Sys_Calendar.CALENDAR QryCal
                ON QryCal.calendar_date = QryLog.LogDate
        WHERE
            LogDate BETWEEN {startdate} and {enddate}
            AND StartTime IS NOT NULL
			Group By 1,2,3,4,5
)ResponseT
 Group By 1,2;
