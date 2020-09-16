/*
#######################################################################
Query2

Query Results File name : CrossReferenceUsage
- Tableau Dashboard Name: Cross-Reference Usage

The following query calculates how much CPU is consumed by a business group when it accesses a certain data domain.
The amount of CPU consumed is equally distributed across the number of subject areas accessed.
How much CPU is consumed by a department when it accesses a certain data domain.
The amount of CPU consumed is equally distributed across the number of data domains accessed.
how much CPU is consumed by a business group when it accesses a certain data domain.
The amount of CPU consumed is equally distributed across the number of data domains accessed.
The followng query is made for excel version lookup table so we have aggregated by Username, Databasename, Tablename

#######################################################################
*/
/*{{save:CrossReferenceUsage.csv}}*/
SELECT
SubDepartment as Username
,DataDomain as "Database Name"
,SubjectArea as Tablename
,ObjectName
,SUM(CPUTimeBusinessGroup) CPUTimeUser
,SUM(CPUTimeDataDomain) CPUTimeDatabase
,SUM(CPUTimeSubjectArea) CPUTimeTable
,SUM(CPUTimeDatabasebTable) CPUTimeDatabasebTable

FROM(

    SELECT
    D.QueryID
	,D.SUMCPUTime/A.SubjectAreaCNT AS CPUTimeSubjectArea
	,D.SUMCPUTime/A.DataDomainCNT AS CPUTimeDataDomain
	,D.SUMCPUTime/A.ObjectNameCNT AS CPUTimeDatabasebTable
	,D.SUMCPUTime as CPUTimeBusinessGroup
    ,D.username as SubDepartment
	,o.ObjectDatabaseName as DataDomain
	,o.ObjectTableName as SubjectArea
    ,o.ObjectName as ObjectName


    FROM

        (SELECT
        QueryID
        ,LogDate
        ,(AMPCPUTime + ParserCPUTime) as SUMCPUTime
		,username as username
        --,Department
        --,SubDepartment
        FROM PDCRINFO.DbqlogTbl_Hst DBQL -- INNER JOIN systemfe.ca_user_xref U
        --    ON DBQL.username = U.username
        WHERE Logdate BETWEEN {startdate} AND {enddate}
				--current_date - 30 AND current_date -1
			) as D

    INNER JOIN

        (SELECT
        QueryId,
        LogDate,
        ObjectDatabaseName,
        ObjectTableName,
		ObjectDatabaseName||'.'||ObjectDatabaseName AS ObjectName
        --DataDomain,
        --SubjectArea
        FROM PDCRINFO.DBQLObjTbl_Hst --INNER JOIN systemfe.ca_table_xref
        --        ON ObjectDatabaseName = DatabaseName
        --        AND ObjectDatabaseName = Tablename
        WHERE Logdate BETWEEN {startdate} AND {enddate}
        AND ObjectType = 'Tab') as O

            ON D.QueryID = O.QueryID
            AND D.Logdate = O.Logdate

    INNER JOIN

        (SELECT
        QueryID
        ,COUNT(DISTINCT ObjectDatabaseName) DataDomainCNT
        ,COUNT(DISTINCT ObjectTableName) SubjectAreaCNT
		,COUNT(DISTINCT ObjectName) ObjectNameCNT
        FROM ( SELECT
                QueryId,
                ObjectDatabaseName,
                ObjectTableName,
                ObjectDatabaseName||'.'||ObjectDatabaseName AS ObjectName
				--DataDomain,
                --SubjectArea
                FROM PDCRINFO.DBQLObjTbl_Hst --INNER JOIN systemfe.ca_table_xref
                --        ON ObjectDatabaseName = DatabaseName
                --        AND ObjectTableName = Tablename
                WHERE Logdate BETWEEN {startdate} AND {enddate}
                AND ObjectType = 'Tab') as A GROUP BY 1)  as A

    ON A.QueryID = D.QueryID
    GROUP BY 1,2,3,4,5,6,7,8,9
                                    ) as F
GROUP BY 1,2,3,4;
