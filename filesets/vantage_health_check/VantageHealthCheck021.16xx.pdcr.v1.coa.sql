
/*
#######################################################################
Query 21
Use the output of following query to create the ca_user_xref categorization username, Department, BusinessGroup

- Select * FROM DBC.Databases

*/
 
/*{{save:ca_user_xref.csv}}*/
Select
              DatabaseName
              ,CreatorName
              ,OwnerName
              ,CreateTimeStamp
              ,LastAlterTimeStamp
FROM DBC.Databases
INNER JOIN

        (Select
        Username as Username
         ,COUNT(*) as NoOfQueries
        From PDCRINFO.DBQLogTbl_hst
        where logdate BETWEEN {startdate} and {enddate}
        Group By 1
        )USR
ON DatabaseName = USR.Username
WHERE DBKind = 'U'
Group BY 1,2,3,4,5;
