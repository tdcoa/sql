/* EXEC DepartmentCPUConsumption ('2019-01-01','2019-01-31')

The DepartmentCPUConsumption macro calculates CPU consumption
and activity by department and sub-department.

parameters:
 - startdate: {startdate}
 - enddate:   {enddate}
 */

/*{{save:DepartmentCPUConsumption.csv}}*/
SELECT
 CAST(D.LogDate AS FORMAT 'YYYY-MM-DD') AS LogDate
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
,UPPER(U.Department) as Department
,UPPER(U.SubDepartment) as BusinessGroup
,SUM(D.AMPCPUTime + D.ParserCPUTime) (BIGINT) as SUMCPUTime
,COUNT(*) as QueryCount
FROM PDCRINFO.DBQLogTbl D
INNER JOIN ca_user_xref U
	ON D.UserName = U.UserName
AND D.LogDate BETWEEN {startdate} AND {enddate}
GROUP BY 1,2,3,4,5
;
