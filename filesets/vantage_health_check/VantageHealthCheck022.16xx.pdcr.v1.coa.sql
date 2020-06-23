
/*
#######################################################################
Query 22
Use the output of following query to create the ca_table_xref categorization Databasename, Tablename, DataDomain,SubjectArea

*/

/*{{save:ca_table_xref.csv}}*/
SELECT  DatabaseName
        ,TableName
        ,AccessCount
        ,LastAccessTimeStamp as LastAccess
         ,CreateTimeStamp
       ,TMP.TableSize as TableSize

FROM    DBC.TablesV
INNER JOIN

        (SELECT           o.objectdatabasename objectdatabasename
                      ,             o.ObjectTableName        as ObjectTableName
                      ,             t.currentperm                                 as  TableSize
                      ,             SUM(o.freqofuse)         as "CountOfUses"
                      FROM PDCRINFO.DBQLObjTbl_hst o
                      LEFT JOIN
                 (
                     Select
           DatabaseName DatabaseName
           ,Tablename Tablename
           ,SUM(CURRENTPERM)/1E9 AS currentperm
            FROM DBC.TableSize
            Group BY 1,2)t
              on o.objectdatabasename = t.DatabaseName
               AND o.ObjectTableName = t.Tablename
        WHERE o.LogDate BETWEEN {startdate} and {enddate}
        Group BY 1,2,3
        ) TMP

        ON DatabaseName = TMP.objectdatabasename

        AND TableName = TMP.ObjectTableName
WHERE   DatabaseName NOT IN ('All', 'Crashdumps', 'DBC', 'dbcmngr',

        'Default', 'External_AP', 'EXTUSER', 'LockLogShredder', 'PUBLIC',

        'Sys_Calendar', 'SysAdmin', 'SYSBAR', 'SYSJDBC', 'SYSLIB',

        'SystemFe', 'SYSUDTLIB', 'SYSUIF', 'TD_SERVER_DB',  'TDStats',

        'TD_SYSGPL', 'TD_SYSXML', 'TDMaps', 'TDPUSER', 'TDQCD',

        'tdwm',  'SQLJ', 'TD_SYSFNLIB',  'SYSSPATIAL')

AND TableKind = 'T'
Group By 1,2,3,4,5,6;
