/* EXEC TopCPUUsers ('2019-01-01','2019-01-31')
The TopCPUUsers macro generates the top CPU consuming users.

parameters:
 - startdate: {startdate}
 - enddate:   {enddate}
 */

/*{{save:TopCPUUsers.csv}}*/
SELECT
ROW_NUMBER() OVER(ORDER BY CPUTime DESC)
,SUM(AMPCPUTime + ParserCPUTime) as CPUTime
,D.Username
,UPPER(C.Department)
,UPPER(C.SubDepartment) as BusinessGroup
FROM PDCRINFO.DBQLogTbl D
    INNER JOIN systemfe.ca_user_xref C
    ON d.username = c.username
WHERE D.Logdate BETWEEN {startdate} AND {enddate}
GROUP BY 3,4,5
ORDER BY 1 ASC;
