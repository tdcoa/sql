/* EXEC DeptDataDomainCPUConsumption ('2019-01-01','2019-01-31')

The DeptDataDomainCPUConsumption macro calculates how much CPU is
consumed by a department when it accesses a certain data domain.
The amount of CPU consumed is equally distributed across the number
of data domains accessed.

parameters:
 - startdate: {startdate}
 - enddate:   {enddate}
*/

/*{{save:DeptDataDomainCPUConsumption.csv}}*/
SELECT SUM(CPUTime)
,Department as Department
,DataDomain

FROM(

    SELECT
    D.QueryID
    ,D.SUMCPUTime/A.DataDomainCNT AS CPUTime
    ,Department
    ,DataDomain

    FROM

        (SELECT
        QueryID
        ,LogDate
        ,(AMPCPUTime + ParserCPUTime) as SUMCPUTime
        ,Department
        ,SubDepartment
        FROM PDCRINFO.DbqlogTbl DBQL INNER JOIN systemfe.ca_user_xref U
            ON DBQL.username = U.username
        WHERE Logdate BETWEEN {startdate} AND {enddate}) as D

    INNER JOIN

        (SELECT
        QueryId,
        LogDate,
        DatabaseName,
        TableName,
        DataDomain,
        SubjectArea
        FROM PDCRINFO.DBQLObjTbl INNER JOIN systemfe.ca_table_xref
                ON ObjectDatabaseName = DatabaseName
                AND ObjectTableName = Tablename
        WHERE Logdate BETWEEN {startdate} AND {enddate}
        AND ObjectType = 'Tab') as O

            ON D.QueryID = O.QueryID
            AND D.Logdate = O.Logdate

    INNER JOIN

        (SELECT
        QueryID
        ,COUNT(DISTINCT DataDomain) DataDomainCNT
        ,COUNT(DISTINCT SubjectArea) SubjectAreaCNT
        FROM ( SELECT
                QueryId,
                DatabaseName,
                TableName,
                DataDomain,
                SubjectArea
                FROM PDCRINFO.DBQLObjTbl INNER JOIN systemfe.ca_table_xref
                        ON ObjectDatabaseName = DatabaseName
                        AND ObjectTableName = Tablename
                WHERE Logdate BETWEEN {startdate} AND {enddate}
                AND ObjectType = 'Tab') as A GROUP BY 1)  as A

    ON A.QueryID = D.QueryID
    GROUP BY 1,2,3,4
                                    ) as F
GROUP BY 2,3
;
