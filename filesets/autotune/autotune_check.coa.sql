/*----------------------------------------------------------------------------------------------------------*/
/*                                                                                                          */
/*  Vantage AutoTune                                                                                        */
/*                                                                                                          */
/*    Version 04.07 Build 2020-06-01                                                                        */
/*    Copyright 2008-2020 Teradata. All rights reserved.                                                    */
/*                                                                                                          */
/*----------------------------------------------------------------------------------------------------------*/
/*                                                                                                          */
/* Consumption Analytics Query For Preliminary Analysis Of Customer AutoTune Potential                      */
/*                                                                                                          */
/* DATE        USER        COMMENTS                                                                         */
/* ----------  ----------  -------------------------------------------------------------------------------  */
/* 2020-06-11  D.ROTH      v04.07 - Original                                                                */
/*----------------------------------------------------------------------------------------------------------*/


/*{{save:autotune_potential.csv}}*/
LOCKING ROW FOR ACCESS
SELECT '{siteid}' SiteID
      ,cast(Current_Date as format 'YYYY-MM-DD') TheDate
      ,Statistics_Type "Statistics Type"
      ,Count(distinct DatabaseName) "Number Databases"
      ,Count(distinct DatabaseName || Tablename ) "Number Tables"
	  ,Count(*) "Missing Statistics"
/*
SELECT DatabaseName
      ,Statistics_Type
      ,Count(*) Number_Instances
*/
FROM
(

/*----------------------------------------------------------------------------------------------------------*/
/*  SUMMARY STATISTICS                                                                                      */
/*                                                                                                          */
/*  V14.00 AND UP COLLECT                                                                                   */
/*----------------------------------------------------------------------------------------------------------*/

SELECT 'Summary' (VarChar(30)) Statistics_Type
      ,'*'   CL
      ,t.DatabaseName DatabaseName
      ,t.TableName    TableName

FROM dbc.tablesV t

LEFT OUTER JOIN DBC.StatsV s
ON t.DatabaseName = s.DatabaseName
AND t.TableName = s.TableName
AND s.statsid = 0

WHERE t.DatabaseName LIKE '%'
  AND t.TableName    LIKE '%'
  AND t.tablekind IN ('T','O','I')
  AND (SELECT CAST(SUBSTR(INFODATA,1,5) as DECIMAL(4,2)) VERSION FROM DBC.DBCINFOV WHERE INFOKEY = 'VERSION') >= 14.00

  AND s.TableName IS NULL

--  AND 'Y' = (SELECT ControlValue (char(01)) FROM AutoTune_Tables.AutoTune_Control WHERE ControlType = 999) -- Use Summary Statistics by Default

GROUP BY 1,2,3,4


UNION


/*----------------------------------------------------------------------------------------------------------*/
/*  BASIC STATISTICS - PRIMARY INDEXES                                                                      */
/*                                                                                                          */
/*  V13.10 AND BEFORE COLLECT                                                                               */
/*  V14.00 AND UP SUMMARY STATS PROVIDES ROW COUNTS                                                         */
/*----------------------------------------------------------------------------------------------------------*/

SELECT 'Primary Index'  (VarChar(30)) Statistics_Type
      ,ColumnLIST   CL
      ,DatabaseName DatabaseName
      ,TableName    TableName

FROM
(
SELECT pti.DatabaseName
      ,pti.TableName
      ,IndexNumber
      ,IndexType
      ,td_sysfnlib.oreplace(TRIM(TRAILING ',' FROM (XMLAGG(TRIM(pti.ColumnName) || ',' ORDER BY pti.ColPos) (VARCHAR(1000)))),' ','') ColumnLIST

  FROM
(
    SELECT DatabaseName,
           TableName,
           IndexNumber,
           IndexType,
           ColumnName,
           ROW_NUMBER () OVER (partition BY DatabaseName, TableName, INDEXnumber ORDER BY DatabaseName, TableName, INDEXnumber, ColumnPosition) ColPos

      FROM DBC.IndicesV t

WHERE t.DatabaseName LIKE '%'
  AND t.TableName    LIKE '%'
  AND indextype IN ('K','P','Q')

) pti

GROUP BY 1,2,3,4
-- ORDER BY 1,2,3,4
) dt

WHERE (dt.DatabaseName, dt.TableName, td_sysfnlib.oreplace('"' || td_sysfnlib.oreplace(td_sysfnlib.oreplace(dt.columnlist,' ',''),',','","') || '"','""','"')) NOT IN
(
SELECT DatabaseName,
       TableName,
       td_sysfnlib.oreplace('"' || td_sysfnlib.oreplace(td_sysfnlib.oreplace(ColumnName,' ',''),',','","') || '"','""','"')
 FROM DBC.StatsV
WHERE DatabaseName LIKE '%'
  AND TableName    LIKE '%'
  AND ColumnName IS NOT NULL
)

 AND (SELECT CAST(SUBSTR(INFODATA,1,5) as DECIMAL(4,2)) VERSION FROM DBC.DBCINFOV WHERE INFOKEY = 'VERSION') < 14.00  -- PIs prior to 14 else summary stats used


UNION


/*----------------------------------------------------------------------------------------------------------*/
/*  BASIC STATISTICS - SECONDARY AND JOIN INDEXES                                                           */
/*----------------------------------------------------------------------------------------------------------*/


SELECT (CASE WHEN indextype IN ('1','2')
             THEN 'Join Index'
             ELSE 'Secondary Index'
             END) (VarChar(30)) Statistics_Type
      ,ColumnLIST   CL
      ,DatabaseName DatabaseName
      ,TableName    TableName

FROM
(
SELECT pti.DatabaseName
      ,pti.TableName
      ,IndexNumber
      ,IndexType
      ,td_sysfnlib.oreplace(TRIM(TRAILING ',' FROM (XMLAGG(TRIM(pti.ColumnName) || ',' ORDER BY pti.ColPos) (VARCHAR(1000)))),' ','') ColumnLIST

  FROM
(
    SELECT DatabaseName,
           TableName,
           IndexNumber,
           IndexType,
           ColumnName,
           ROW_NUMBER () OVER (partition BY DatabaseName, TableName, INDEXnumber ORDER BY DatabaseName, TableName, INDEXnumber, ColumnPosition) ColPos

      FROM DBC.IndicesV t

WHERE t.DatabaseName LIKE '%'
  AND t.TableName    LIKE '%'
  AND indextype NOT IN ('K','P','Q','J')

) pti

GROUP BY 1,2,3,4
-- ORDER BY 1,2,3,4
) dt

WHERE (dt.DatabaseName, dt.TableName, td_sysfnlib.oreplace('"' || td_sysfnlib.oreplace(td_sysfnlib.oreplace(dt.columnlist,' ',''),',','","') || '"','""','"')) NOT IN
(
SELECT DatabaseName,
       TableName,
       td_sysfnlib.oreplace('"' || td_sysfnlib.oreplace(td_sysfnlib.oreplace(ColumnName,' ',''),',','","') || '"','""','"')
  FROM DBC.StatsV t
 WHERE t.DatabaseName LIKE '%'
   AND t.TableName    LIKE '%'
   AND ColumnName IS NOT NULL
)


UNION


/*----------------------------------------------------------------------------------------------------------*/
/*  BASIC STATISTICS - partitions AND PARTIONING Columns                                                    */
/*                                                                                                          */
/*  V14.00 AND UP COLLECT PARTITONED TABLES ONLY                                                            */
/*----------------------------------------------------------------------------------------------------------*/


SELECT 'Partitions'  (VarChar(30)) Statistics_Type
      ,'partition'     CL
      ,DA.DatabaseName DatabaseName
      ,DA.TableName    TableName

FROM
(
SELECT p.DatabaseName,
       p.TableName,
       'partition' ColumnName

 FROM DBC.indexconstraintsV p

WHERE p.DatabaseName LIKE '%'
  AND p.TableName    LIKE '%'

GROUP BY 1,2,3
) DA

LEFT OUTER JOIN DBC.StatsV s
  on DA.DatabaseName = s.DatabaseName
 AND DA.TableName = s.TableName
 AND DA.ColumnName = s.ColumnName

WHERE s.ColumnName IS NULL


UNION

SELECT 'Partitions'  (VarChar(30)) Statistics_Type
      ,DA.ColumnName CL
      ,DA.DatabaseName     DatabaseName
      ,DA.TableName        TableName

FROM dbc.ColumnsV DA

LEFT OUTER JOIN DBC.StatsV s
  on DA.DatabaseName = s.DatabaseName
 AND DA.TableName = s.TableName
 AND DA.ColumnName = s.ColumnName

WHERE DA.DatabaseName LIKE '%'
  AND DA.TableName    LIKE '%'

  AND DA.partitioningColumn = 'Y'
  AND DA.ColumnType Not In ('UT', 'PD', 'PM', 'PS', 'PT', 'PZ') -- ignore column types that cannot have statistics

  AND s.ColumnName IS NULL


UNION

-- if Partitoning Column is Not part of Primary Index add stats on Partiton, PI

SELECT 'Partitions'  (VarChar(30)) Statistics_Type
      ,DA.ColumnLIST       CL
      ,DA.DatabaseName     DatabaseName
      ,DA.TableName        TableName

FROM
(
SELECT pi.ColumnLIST
      ,pi.DatabaseName
      ,pi.TableName

FROM
(
SELECT pti.DatabaseName
      ,pti.TableName
      ,IndexNumber
      ,IndexType
      ,td_sysfnlib.oreplace(TRIM(TRAILING ',' FROM (XMLAGG(TRIM(pti.ColumnName) || ',' ORDER BY pti.ColPos) (VARCHAR(1000)))),' ','') ColumnLIST

  FROM
(
    SELECT DatabaseName,
           TableName,
           IndexNumber,
           IndexType,
           ColumnName,
           ROW_NUMBER () OVER (partition BY DatabaseName, TableName, INDEXnumber ORDER BY DatabaseName, TableName, INDEXnumber, ColumnPosition) ColPos

      FROM DBC.IndicesV t

WHERE t.DatabaseName LIKE '%'
  AND t.TableName    LIKE '%'
  AND indextype IN ('Q')
)  pti

GROUP BY 1,2,3,4
) pi

LEFT OUTER JOIN
(
-- grab partitioning Column

SELECT DA.DatabaseName,
       DA.TableName,
       DA.ColumnName

FROM dbc.ColumnsV DA

LEFT OUTER JOIN DBC.StatsV s
  on DA.DatabaseName = s.DatabaseName
 AND DA.TableName = s.TableName
 AND DA.ColumnName = s.ColumnName

WHERE DA.DatabaseName LIKE '%'
  AND DA.TableName    LIKE '%'
  AND DA.partitioningColumn = 'Y'
  AND DA.ColumnType Not In ('UT', 'PD', 'PM', 'PS', 'PT', 'PZ') -- ignore column types that cannot have statistics

) pc
 on pi.DatabaseName = pc.DatabaseName
AND pi.TableName = pc.TableName
AND POSITION(  pc.ColumnName   IN   pi.ColumnLIST  ) > 0

WHERE pc.TableName IS NULL

) DA

LEFT OUTER JOIN DBC.StatsV s
  on DA.DatabaseName = s.DatabaseName
 AND DA.TableName = s.TableName
 AND ('partition,' || DA.columnlist) = s.ColumnName

WHERE s.ColumnName IS NULL


UNION


/*----------------------------------------------------------------------------------------------------------*/
/*  BASIC STATISTICS - PERIOD BEGIN AND END TIMESTAMPS                                                      */
/*----------------------------------------------------------------------------------------------------------*/

SELECT 'Temporal'  (VarChar(30)) Statistics_Type
      ,DA.ColumnName   CL
      ,DA.DatabaseName DatabaseName
      ,DA.TableName    TableName
/*
SELECT DatabaseName
      ,TableName
      ,ColumnName
      ,NewColumnName
      ,NewStatsName
*/
FROM
(
SELECT c.DatabaseName,
       c.TableName,
       c.ColumnName,
       'BEGIN(' || TRIM(c.ColumnName)   || ')' NewColumnName,
       'BEGIN_' || TRIM(c.ColumnName) NewStatsName

FROM dbc.tablesV t,
     dbc.ColumnsV c

WHERE t.tablekind IN ('T','O','I')
  AND t.DatabaseName = c.DatabaseName
  AND t.TableName    = c.TableName
  AND t.DatabaseName LIKE '%'
  AND t.TableName    LIKE '%'

  AND c.columntype = 'PM'

-- if the period Column does not direct statistics on it
-- and there are no statistics on the BEGIN variant, add BEGIN

  AND (c.DatabaseName, c.TableName, NewColumnName) not iN
  (
-- for some reason, beg-end stats are saved with space before final parens, so strip that out for compare
   SELECT DatabaseName, TableName, td_sysfnlib.oreplace(ColumnName,' ','') ColumnName
--         ,StatsName
     FROM dbc.StatsV
   WHERE ColumnName like 'BEGIN(%'
     AND DatabaseName LIKE '%'
     AND TableName    LIKE '%'
  )

  AND (c.DatabaseName, c.TableName, c.ColumnName) not iN
  (
   SELECT DatabaseName, TableName, ColumnName
     FROM dbc.StatsV
   WHERE DatabaseName LIKE '%'
     AND TableName    LIKE '%'
  )

  AND (c.DatabaseName, c.TableName, NewStatsName) not iN
  (
   SELECT DatabaseName, TableName, ColumnName
     FROM dbc.ColumnsV
   WHERE DatabaseName LIKE '%'
     AND TableName    LIKE '%'
  )

) DA

UNION

SELECT 'Temporal'  (VarChar(30)) Statistics_Type
      ,DA.ColumnName   CL
      ,DA.DatabaseName DatabaseName
      ,DA.TableName    TableName

FROM
(
SELECT c.DatabaseName,
       c.TableName,
       c.ColumnName,
       'END(' || TRIM(c.ColumnName)   || ')' NewColumnName,
       'END_' || TRIM(c.ColumnName) NewStatsName

FROM dbc.tablesV t,
     dbc.ColumnsV c

WHERE t.tablekind IN ('T','O','I')
  AND t.DatabaseName = c.DatabaseName
  AND t.TableName    = c.TableName
  AND t.DatabaseName LIKE '%'
  AND t.TableName    LIKE '%'

  AND c.columntype = 'PM'

-- if the period Column does not direct statistics on it
-- and there are no statistics on the END variant, add END

  AND (c.DatabaseName, c.TableName, NewColumnName) not iN
  (
-- for some reason, beg-end stats are saved with space before final parens, so strip that out for compare
   SELECT DatabaseName, TableName, td_sysfnlib.oreplace(ColumnName,' ','') ColumnName
--         ,StatsName
     FROM dbc.StatsV
   WHERE ColumnName like 'END(%'
     AND DatabaseName LIKE '%'
     AND TableName    LIKE '%'
  )

  AND (c.DatabaseName, c.TableName, c.ColumnName) not iN
  (
   SELECT DatabaseName, TableName, ColumnName
     FROM dbc.StatsV
   WHERE DatabaseName LIKE '%'
     AND TableName    LIKE '%'
  )

  AND (c.DatabaseName, c.TableName, NewStatsName) not iN
  (
   SELECT DatabaseName, TableName, ColumnName
     FROM dbc.ColumnsV
   WHERE DatabaseName LIKE '%'
     AND TableName    LIKE '%'
  )

) DA


UNION


/*----------------------------------------------------------------------------------------------------------*/
/*  BASIC STATISTICS - SOFT RI Columns - CHILD                                                              */
/*----------------------------------------------------------------------------------------------------------*/


SELECT 'Soft-RI'  (VarChar(30)) Statistics_Type
      ,child_columnlist  CL
      ,ChildDB           DatabaseName
      ,ChildtABLE        TableName

FROM
(
SELECT  ChildDB
       ,ChildTable
       ,td_sysfnlib.oreplace(TRIM(TRAILING ',' FROM (XMLAGG(TRIM(ChildKeyColumn) || ',' ORDER BY ColumnPosition) (VARCHAR(1000)))),' ','') child_columnlist

       ,ParentDB
       ,ParentTable
       ,td_sysfnlib.oreplace(TRIM(TRAILING ',' FROM (XMLAGG(TRIM(ParentKeyColumn) || ',' ORDER BY ColumnPosition) (VARCHAR(1000)))),' ','') parent_columnlist

       ,IndexId

FROM
(
SELECT RANK() OVER (partition BY ChildDB, ChildTable, IndexId ORDER BY ChildDB, ChildTable, IndexId, Columnid ) ColumnPosition,

       ChildDB,
       ChildTable,
       ChildKeyColumn,

       ParentDB,
       ParentTable,
       ParentKeyColumn,

       IndexId

FROM dbc.all_ri_childrenV r,
     dbc.ColumnsV c

WHERE childdb     LIKE '%'
  AND ChildTable  LIKE '%'

  AND childdb = c.DatabaseName
  AND childtable = c.TableName
  AND childkeycolumn = c.ColumnName

) dt

GROUP BY 1,2,4,5,7

) dt2

LEFT OUTER JOIN DBC.StatsV s
  on dt2.childdb = s.DatabaseName
 AND dt2.childtable = s.TableName
 AND dt2.child_columnlist = s.ColumnName

WHERE dt2.childdb LIKE '%'

AND s.ColumnName IS NULL

GROUP BY 1,2,3,4


UNION


/*----------------------------------------------------------------------------------------------------------*/
/*  BASIC STATISTICS - SOFT RI Columns - PARENT                                                             */
/*----------------------------------------------------------------------------------------------------------*/


SELECT 'Soft-RI'  (VarChar(30)) Statistics_Type
      ,parent_columnlist  CL
      ,ParentDB           DatabaseName
      ,ParenttABLE        TableName

FROM
(

SELECT  ChildDB
       ,ChildTable
       ,td_sysfnlib.oreplace(TRIM(TRAILING ',' FROM (XMLAGG(TRIM(ChildKeyColumn) || ',' ORDER BY ColumnPosition) (VARCHAR(1000)))),' ','') child_columnlist

       ,ParentDB
       ,ParentTable
       ,td_sysfnlib.oreplace(TRIM(TRAILING ',' FROM (XMLAGG(TRIM(ParentKeyColumn) || ',' ORDER BY ColumnPosition) (VARCHAR(1000)))),' ','') parent_columnlist

	  ,IndexId

FROM
(
SELECT RANK() OVER (partition BY ChildDB, ChildTable, IndexId ORDER BY ChildDB, ChildTable, IndexId, Columnid ) ColumnPosition,

       ChildDB,
       ChildTable,
       ChildKeyColumn,

       ParentDB,
       ParentTable,
       ParentKeyColumn,

       IndexId

FROM dbc.all_ri_childrenV r,
     dbc.ColumnsV c

WHERE childdb     LIKE '%'
  AND childtable  LIKE '%'

  AND childdb = c.DatabaseName
  AND childtable = c.TableName
  AND childkeycolumn = c.ColumnName
) dt

GROUP BY 1,2,4,5,7
) dt2

LEFT OUTER JOIN DBC.StatsV s
  on dt2.Parentdb = s.DatabaseName
 AND dt2.Parenttable = s.TableName
 AND dt2.Parent_columnlist = s.ColumnName

WHERE dt2.Parentdb LIKE '%'

AND s.ColumnName IS NULL

GROUP BY 1,2,3,4

) DT

where DatabaseName Not In
(
 'all'
,'console'
,'crashdumps'
,'dbc'
,'dbcmanager'
,'dbcmngr'
,'default'
,'external_ap'
,'extuser'
,'locklogshredder'
,'pdcrdata'
,'pdcrstg'
,'pdcrtpcd'
,'pdcrinfo'
,'public'
,'sqlj'
,'statisticsmanager'
,'sysadmin'
,'sysbar'
,'sysdba'
,'sysjdbc'
,'sysspatial'
,'systemfe'
,'sysudtlib'
,'sysuif'
,'sys_calendar'
,'tdmaps'
,'tdpuser'
,'tdqcd'
,'tdstats'
,'tdwm'
,'td_server_db'
,'td_sysfnlib'
,'td_sysgpl'
,'td_sysxml'
,'viewpoint'
)

Group by 1,2,3

Order by 1,2,3
;

/*{{pptx:autotune_check.pptx}}*/;
