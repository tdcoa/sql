/* Compete Snowflake set
Parameters:
 - startdate, enddate, siteid

 Dependencies:
 - db_objects
*/

/*{{save:dat_snowflake_tablekind.csv}}*/
SELECT
     '{siteid}' as Site_ID
    ,CAST(CAST(SUM(CASE WHEN TableKindDesc = 'Join Index' THEN ObjectCount ELSE 0 END) AS FORMAT 'ZZZ,ZZZ,ZZ9') AS VARCHAR(20)) AS JICount
    ,CAST(CAST(SUM(CASE WHEN TableKindDesc = 'Queue Table' THEN ObjectCount ELSE 0 END) AS FORMAT 'ZZZ,ZZZ,ZZ9') AS VARCHAR(20)) AS QueueCount
FROM table_kinds_by_database
WHERE DatabaseName NOT IN  (select dbname from dim_tdinternal_databases)
;


/*{{save:dat_snowflake_table_multiset.csv}}*/
select
     '{siteid}' as Site_ID
    ,CAST(CAST(SUM(CASE WHEN MultisetInd = 'N' THEN ObjectCount ELSE 0 END) AS FORMAT 'ZZZ,ZZZ,ZZ9') AS VARCHAR(20)) as SetTableCount
    ,CAST(CAST(SUM(ObjectCount) AS FORMAT 'ZZZ,ZZZ,ZZ9') AS VARCHAR(20)) as TotalObjectCount
    ,CAST(SUM(CASE WHEN MultisetInd = 'N' THEN ObjectCount ELSE 0 END) / CAST(SUM(ObjectCount) AS DECIMAL(18,3)) * 100 AS INTEGER FORMAT 'ZZ9') AS SetTablePct
FROM table_kinds_by_database
;


/*{{save:dat_snowflake_col_types.csv}}*/
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
,CAST(CAST(SUM(CASE WHEN IdentityColumnType in ('GA','GD') THEN ColumnCount ELSE 0 END) AS FORMAT 'ZZZ,ZZZ,ZZ9') AS VARCHAR(20)) AS IdentityCnt
from column_types
;


/*{{save:dat_snowflake_indextype.csv}}*/
select
   '{siteid}' as Site_ID
  ,CAST(CAST(SUM(CASE WHEN IndexTypeBucket = 'Primary Index' THEN IndexCount ELSE 0 END) AS FORMAT 'ZZZ,ZZZ,ZZ9') AS VARCHAR(20)) AS UPINUPI
  ,CAST(CAST(SUM(CASE WHEN IndexTypeBucket = 'Partition' THEN IndexCount ELSE 0 END) AS FORMAT 'ZZZ,ZZZ,ZZ9') AS VARCHAR(20)) AS PPI
/* Unique constraints are included as SI's - in temporal tables they are JI's.
   PK not included as SI's - they are listed under 'constraints' */
  ,CAST(CAST(SUM(CASE WHEN IndexTypeBucket = 'Secondary Index' THEN IndexCount ELSE 0 END) AS FORMAT 'ZZZ,ZZZ,ZZ9') AS VARCHAR(20)) AS SI
from index_types_by_database
Where DatabaseName NOT IN  (select dbname from dim_tdinternal_databases);


/*{{save:dat_snowflake_constrainttype.csv}}*/
SELECT
 '{siteid}' as Site_ID
 ,CAST(CAST(SUM(CASE WHEN ConstraintType = 'Column Constraint' THEN ConstraintCount ELSE 0 END) AS FORMAT 'ZZZ,ZZZ,ZZ9') AS VARCHAR(20)) AS CCC_Count
 ,CAST(CAST(SUM(CASE WHEN ConstraintType = 'Primary Key' THEN ConstraintCount ELSE 0 END) AS FORMAT 'ZZZ,ZZZ,ZZ9') AS VARCHAR(20)) AS PKC_Count
 ,CAST(CAST(SUM(CASE WHEN ConstraintType = 'Foreign Key' THEN ConstraintCount ELSE 0 END) AS FORMAT 'ZZZ,ZZZ,ZZ9') AS VARCHAR(20)) AS FKC_Count
FROM constraint_type_by_database
WHERE DatabaseName NOT IN  (select dbname from dim_tdinternal_databases)
;


/*{{save:dat_snowflake_usage_per_type.csv}}*/
SELECT
     '{siteid}' as Site_ID
    ,CAST(CAST(SUM(CASE WHEN FT.ObjectType = 'Tmp' THEN OT.Frequency_of_Use ELSE 0 END) AS FORMAT 'ZZZ,ZZZ,ZZ9') AS VARCHAR(20)) AS GTTUsage
    ,CAST(CAST(SUM(CASE WHEN FT.ObjectType IN ('Idx', 'HIx') THEN OT.ObjectCount ELSE 0 END) AS FORMAT 'ZZZ,ZZZ,ZZ9') AS VARCHAR(20)) AS IndexCount
    ,CAST(CAST(SUM(CASE WHEN FT.ObjectType IN ('Idx', 'HIx') THEN OT.Frequency_of_Use ELSE 0 END) AS FORMAT 'ZZZ,ZZZ,ZZ9') AS VARCHAR(20)) AS IndexUsage
FROM
    dim_dbobject FT
    LEFT OUTER JOIN
    (
        SELECT
             ObjectType
            ,COUNT(DISTINCT ObjectDatabaseName || ObjectTableName || TRIM(ObjectNum)) AS ObjectCount
            ,SUM(CAST(FreqofUse AS BIGINT)) AS Frequency_of_Use
        FROM
            pdcrinfo.DBQLObjTbl OT
        WHERE
            LogDate BETWEEN {startdate} AND {enddate}
            AND OT.ObjectDatabaseName NOT IN (select dbname from dim_tdinternal_databases)
            AND ObjectType IN ('Tmp', 'Idx', 'HIx')   -- Join Indexes not included, should they be?
        GROUP BY 1
    ) OT
    ON FT.ObjectType = OT.ObjectType
;

