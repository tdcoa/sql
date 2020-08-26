/* Compete Snowflake set
Parameters:
 - startdate, enddate, siteid, dbqobjtbl
*/


/* LOAD DIMENSIONS INTO VOLATILE TABLES:
*/;

/*{{temp:dim_dbobject.csv}}*/;
/*{{temp:dim_tdinternal_databases.csv}}*/;


/*{{save:dat_snowflake_tablekind.csv}}*/
SELECT
     '{siteid}' as Site_ID                    
    ,CAST(CAST(SUM(ObjectCount) AS FORMAT 'ZZZ,ZZZ,ZZ9') AS VARCHAR(20)) AS JICount
FROM table_kinds_by_database
WHERE DatabaseName NOT IN  (select dbname from "dim_tdinternal_databases.csv")
  AND TableKindDesc = 'Join Index'
;                           


/*{{save:dat_snowflake_table_multiset.csv}}*/
select 
     '{siteid}' as Site_ID
    ,CAST(CAST(SUM(CASE WHEN MultisetInd = 'N' THEN ObjectCount ELSE 0 END) AS FORMAT 'ZZZ,ZZZ,ZZ9') AS VARCHAR(20)) as SetTableCount
    ,CAST(CAST(SUM(ObjectCount) AS FORMAT 'ZZZ,ZZZ,ZZ9') AS VARCHAR(20)) as TotalObjectCount
    ,CAST(SUM(CASE WHEN MultisetInd = 'N' THEN ObjectCount ELSE 0 END) / CAST(SUM(ObjectCount) AS DECIMAL(18,3)) * 100 AS INTEGER FORMAT 'ZZ9') AS SetTablePct
FROM table_kinds_by_database
;
 

/*{{save:dat_snowflake_special_data_types.csv}}*/
select 
 '{siteid}' as Site_ID
,CAST(CAST(SUM(CASE WHEN ColumnCategory = 'INTERVAL' THEN ColumnCount ELSE 0 END) AS FORMAT 'ZZZ,ZZZ,ZZ9') AS VARCHAR(20)) AS IntervalCnt
,CAST(CAST(SUM(CASE WHEN ColumnCategory = 'PERIOD' THEN ColumnCount ELSE 0 END) AS FORMAT 'ZZZ,ZZZ,ZZ9') AS VARCHAR(20)) AS PeriodCnt
,CAST(CAST(SUM(CASE WHEN ColumnCategory = 'NUMBER' THEN ColumnCount ELSE 0 END) AS FORMAT 'ZZZ,ZZZ,ZZ9') AS VARCHAR(20)) AS NumberCnt
,CAST(CAST(SUM(CASE WHEN ColumnCategory = 'BLOB' AND 
                         CAST(SUBSTRING(COLUMNTYPE FROM 6 FOR CHARS(COLUMNTYPE) -6) AS BIGINT) > 8*2**20  
                    THEN ColumnCount ELSE 0 END) AS FORMAT 'ZZZ,ZZZ,ZZ9') AS VARCHAR(20)) AS Blob8MCnt
,CAST(CAST(SUM(CASE WHEN ColumnCategory = 'CLOB' AND 
                         CAST(SUBSTRING(COLUMNTYPE FROM 6 FOR INDEX(COLUMNTYPE, ')') -6) AS BIGINT) *
                         CASE WHEN INDEX(COLUMNTYPE, 'LATIN') > 0 THEN 1 ELSE 2 END  > 16*2**20 
                    THEN ColumnCount ELSE 0 END) AS FORMAT 'ZZZ,ZZZ,ZZ9') AS VARCHAR(20)) AS Clob16MCnt
,CAST(CAST(SUM(CASE WHEN ColumnCategory = 'XML' OR ColumnCategory LIKE 'JSON%' THEN ColumnCount ELSE 0 END) AS FORMAT 'ZZZ,ZZZ,ZZ9') AS VARCHAR(20)) AS XMLJSONCnt
,CAST(CAST(SUM(CASE WHEN ColumnCategory = 'ST_GEOMETRY' THEN ColumnCount ELSE 0 END) AS FORMAT 'ZZZ,ZZZ,ZZ9') AS VARCHAR(20)) AS GeoCnt
from column_types
;


/*{{save:dat_snowflake_indextype.csv}}*/
select
   '{siteid}' as Site_ID 
  ,CAST(CAST(SUM(CASE WHEN IndexTypeDesc = 'Unique Primary Index (UPI)' THEN IndexCount ELSE 0 END) AS FORMAT 'ZZZ,ZZZ,ZZ9') AS VARCHAR(20)) AS UPI 
  ,CAST(CAST(SUM(CASE WHEN IndexTypeDesc LIKE 'Partitioned Primary Index%' THEN IndexCount ELSE 0 END) AS FORMAT 'ZZZ,ZZZ,ZZ9') AS VARCHAR(20)) AS PPI 
from index_types_by_database
Where DatabaseName NOT IN  (select dbname from "dim_tdinternal_databases.csv");

                                                                           
/*{{save:dat_snowflake_constrainttype.csv}}*/
SELECT  
 '{siteid}' as Site_ID
 ,CAST(CAST(SUM(CASE WHEN ConstraintType = 'Column Constraint' THEN ConstraintCount ELSE 0 END) AS FORMAT 'ZZZ,ZZZ,ZZ9') AS VARCHAR(20)) AS CCC_Count
 ,CAST(CAST(SUM(CASE WHEN ConstraintType = 'Primary Key' THEN ConstraintCount ELSE 0 END) AS FORMAT 'ZZZ,ZZZ,ZZ9') AS VARCHAR(20)) AS PKC_Count 
 ,CAST(CAST(SUM(CASE WHEN ConstraintType = 'Foreign Key' THEN ConstraintCount ELSE 0 END) AS FORMAT 'ZZZ,ZZZ,ZZ9') AS VARCHAR(20)) AS FKC_Count 
FROM constraint_type_by_database
WHERE DatabaseName NOT IN  (select dbname from "dim_tdinternal_databases.csv")      
;


/*{{save:dat_snowflake_usage_per_type.csv}}*/
SELECT
     '{siteid}' as Site_ID
    ,FT.ObjectType
    ,ObjectTypeDesc
    ,CAST(CAST(ZEROIFNULL(Frequency_of_Use) AS FORMAT 'ZZZ,ZZZ,ZZ9') AS VARCHAR(20)) AS Frequency_of_Use
FROM
    "dim_dbobject.csv" FT
    LEFT OUTER JOIN
    (
        SELECT
            ObjectType
            ,SUM(CAST(FreqofUse AS BIGINT)) AS Frequency_of_Use
        FROM
            pdcrinfo.DBQLObjTbl OT
        WHERE
            LogDate BETWEEN {startdate} AND {enddate}  
            AND OT.ObjectDatabaseName NOT IN (select dbname from "dim_tdinternal_databases.csv")
            AND ObjectType = 'Tmp'   -- Global Temp only
        GROUP BY 1
    ) OT
    ON FT.ObjectType = OT.ObjectType
WHERE FT.ObjectType = 'Tmp'          -- dim_dboject has all obj types but this resultset is only for Tmp
;

/*{{pptx:snowflake_migration_blockers.pptx}}*/
