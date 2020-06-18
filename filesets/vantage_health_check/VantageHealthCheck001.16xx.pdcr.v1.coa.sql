
/*
#######################################################################
Query1

Query Results File name : DepartmentCPUConsumption
- Tableau Dashboard Name: Dashboard1, Dashboard2, Dashboard3

#######################################################################
*/

/*{{save:DepartmentCPUConsumption.csv}}*/
	SELECT
	D.LogDate
	,StatementType
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
	,D.username as Username
	,UPPER(D.Username) as Department
	,UPPER(D.Username) as BusinessGroup
	,SUM(D.AMPCPUTime + D.ParserCPUTime) (BIGINT) as SUMCPUTime
	,COUNT(*) as QueryCount
	FROM PDCRINFO.DBQLogTbl_Hst D
	/*INNER JOIN systemfe.ca_user_xref U
		ON D.UserName = U.UserName*/
	WHERE D.LogDate BETWEEN {startdate} AND {enddate}
	GROUP BY 1,2,3,4,5,6;
