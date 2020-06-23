/*
##############################################3
Query 16

Query Output File Name: HighActivityUsers
Tableau Dashboard: HighActivityUsers, ExportByUser, CPU Dist By Query USage

*/

/*{{save:HighActivityUsers.csv}}*/
SELECT
	QryLog.LogDate	AS "Log Date"
	,Extract( Hour from starttime) AS "Log Hour"
	,username as UserName
	,CASE WHEN StatementType = 'Merge Into' THEN 'Ingest & Prep'
		  WHEN StatementType = 'Begin Loading' THEN 'Ingest & Prep'
		  WHEN StatementType = 'Mload' THEN 'Ingest & Prep'
		  WHEN StatementType = 'Collect Statistics' THEN 'Data Maintenance'
		  WHEN StatementType = 'Delete' THEN 'Ingest & Prep'
		  WHEN StatementType = 'End Loading' THEN 'Ingest & Prep'
		  WHEN StatementType = 'Begin Delete Mload' THEN 'Ingest & Prep'
		  WHEN StatementType = 'Update' THEN 'Ingest & Prep'
		  WHEN StatementType = 'Select' THEN 'Answers'
		  WHEN StatementType = 'Exec' THEN 'Ingest & Prep'
		  WHEN StatementType = 'Release Mload' THEN 'Ingest & Prep'
		  WHEN StatementType = 'Insert' THEN 'Ingest & Prep'
		  WHEN StatementType = 'Begin Mload' THEN 'Ingest & Prep'
		  WHEN StatementType = 'Execute Mload' THEN 'Ingest & Prep'
		  WHEN StatementType = 'Commit Work' THEN 'Ingest & Prep'
		  ELSE 'System/Procedural' END AS StatementOutcome
	,StatementType
	,clientid AS ClientId
	,CASE
                WHEN (QryLog.StatementType IN ('Insert', 'Update', 'Delete', 'Create Table', 'Merge Into')
					OR QryLog.AppID LIKE ANY('TPTLOAD%', 'TPTUPD%', 'FASTLOAD%', 'MULTLOAD%', 'EXECUTOR%', 'JDBCL%'))
                    THEN 'ETL/ELT'
                WHEN QryLog.StatementType = 'Select'
                    AND (AppID IN ('TPTEXP', 'FASTEXP') or appid like  'JDBCE%')
                    THEN 'EXPORT'
                WHEN QryLog.StatementType = 'Select'
                    AND QryLog.AppID NOT LIKE ANY('TPTLOAD%', 'TPTUPD%', 'FASTLOAD%', 'MULTLOAD%', 'EXECUTOR%', 'JDBCL%', 'JDBCE%')
                    THEN 'QUERY'
				--WHEN QryLog.StatementType in ('Dump Database','Unrecognized type','Release Lock','Collect Statistics')
				    --THEN 'ADMIN'
                ELSE
                    'OTHER'
            END AS WorkLoadType
	,'Complexity' AS Complexity
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
	FROM PDCRINFO.DBQLogTbl_hst QryLog
	INNER JOIN Sys_Calendar.CALENDAR  c
	ON QryLog.LogDate = c.Calendar_date
	--INNER JOIN systemfe.ca_user_xref U
	--	ON QryLog.UserName = U.UserName
	WHERE QryLog.LogDate BETWEEN {startdate} and {enddate}
	and c.Calendar_date BETWEEN {startdate} and {enddate}

	--	ON QryLog.UserName = U.UserName

	GROUP BY 1,2,3,4,5,6,7,8,9;
