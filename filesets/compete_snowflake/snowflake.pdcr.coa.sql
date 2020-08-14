/* 
  parameters:
  - iopernode_90pct = 1000  (from archie)
*/


/* LOAD DIMENSIONS INTO VOLATILE TABLES:
*/;

/*{{temp:dim_dbobject.csv}}*/;
/*{{temp:dim_tdinternal_databases.csv}}*/;


/*{{save:dat_dbobject_count_per_tablekind.csv}}*/
SELECT
     '{siteid}' as Site_ID
    ,'Object Type Definitions' AS ReportName
    ,Table_Bucket
    ,TableKind_Desc                     
    ,SUM(ObjectCount)AS ObjectCount
FROM table_kinds_by_database
WHERE DatabaseName NOT IN
  (select dbname from "dim_tdinternal_databases.csv")
GROUP BY 3,4;                            


/*{{save:dat_dbobject_table_multiset.csv}}*/
select 
     '{siteid}' as Site_ID
    ,CASE MultisetInd
       WHEN 'Y' THEN 'MULTISET Tables'
       WHEN 'N' THEN 'SET Tables'
       ELSE 'Others' 
     END AS Table_Type
    ,SUM(ObjectCount) as Total_Count
FROM table_kinds_by_database
WHERE DatabaseName NOT IN (select dbname from "dim_tdinternal_databases.csv")  
GROUP BY 2;


/*{{save:dat_dbobject_usage_per_type.csv}}*/
SELECT
     '{siteid}' as Site_ID
    ,'Object Types Used and Frequency' AS ReportName
    ,FT.ObjectType
    ,ObjectTypeDesc
    ,ZEROIFNULL(Frequency_of_Use) AS Frequency_of_Use
FROM
    "dim_dbobject.csv" FT
    LEFT OUTER JOIN
    (
        SELECT
            ObjectType
            ,SUM(CAST(FreqofUse AS BIGINT)) AS Frequency_of_Use
        FROM
            PDCRINFO.DBQLObjTbl OT
        WHERE
            --jcm
            LogDate BETWEEN {startdate} AND {enddate}  
            AND OT.ObjectDatabaseName NOT IN (select dbname from "dim_tdinternal_databases.csv")      
        GROUP BY 1
    ) OT
    ON FT.ObjectType = OT.ObjectType
ORDER BY ObjectTypeDesc;


/*{{save:dat_dbobject_count_per_datatype.csv}}*/
SELECT
     '{siteid}' as Site_ID
    ,'Data Type Usage' AS ReportName
    ,DataTypeDesc
    ,SUM(ColumnCount) AS Total_Cnt
FROM column_types
GROUP BY 3
ORDER BY 3;


/*{{save:dat_dbobject_count_per_statementtype.csv}}*/
SELECT
     '{siteid}' as Site_ID
    ,'SQL Statement Type Usage' AS ReportName
    ,StatementType
    ,COUNT(*) AS Frequency_Count
FROM
    PDCRINFO.DBQLogTbl
WHERE
    LogDate BETWEEN {startdate} AND {enddate}
GROUP BY 3;


/*{{save:dat_dbobject_count_per_indextype.csv}}*/
/*{{pptx:snowflake_migration_blockers.pptx}}*/
select
   '{siteid}' as Site_ID 
  ,'Index Types' AS ReportName
  ,IndexTypeDesc
  ,SUM(IndexCount) AS Total
from index_types_by_database
Where DatabaseName NOT IN  (select dbname from "dim_tdinternal_databases.csv")      
group by 3;

                                                                           
/*{{save:dat_dbobject_count_per_constrainttype.csv}}*/
SELECT  
 '{siteid}' as Site_ID
 ,'Constraint Analysis' as ReportName
 ,ConstraintType
 ,Count(*) AS ConstraintCount
FROM constraint_details
WHERE DatabaseName NOT IN  (select dbname from "dim_tdinternal_databases.csv")      
GROUP BY 3;
