/* dimension  for index types
*/

/*{{temp:dim_indexkind.csv}}*/;


create volatile table index_types as 
(
 SEL
      Inds.DatabaseName
     ,IK.IndexKindDesc AS IndexTypeDesc
     ,COALESCE(COUNT(*), 0) AS IndexCount
 FROM "dim_indexkind.csv" AS IK
 LEFT JOIN DBC.IndicesV Inds
   ON IK.IndexKind = Inds.IndexType 
  AND IK.UniqueFlag = Inds.UniqueFlag
from DBC.IndicesV Inds
group by 1,2
) with data 
no primary index on commit preserve rows;

/*
create volatile table index_types as 
(
SELECT
   DatabaseName,
   CASE
        WHEN Inds.IndexType = 'P'
            AND Inds.UniqueFlag = 'Y'
            THEN 'Unique Primary Index (UPI)'
        WHEN Inds.IndexType = 'P'
            AND Inds.UniqueFlag = 'N'
            THEN 'Non-Unique Primary Index (NUPI)'
        WHEN Inds.IndexType = 'Q'
            THEN 'Partitioned Primary Index'
        WHEN Inds.IndexType = 'A'
            THEN 'Primary AMP Index'
        WHEN Inds.IndexType = 'S'
            AND Inds.UniqueFlag = 'Y'
            THEN 'Unique Secondary Index (USI)'
        WHEN Inds.IndexType = 'S'
            AND Inds.UniqueFlag = 'N'
            THEN 'Non-Unique Secondary Index (NUSI)'
        WHEN Inds.IndexType = 'U'
            THEN 'Unique Secondary with NOT NULL'
        WHEN Inds.IndexType = 'K'
            THEN 'Primary Key'
        WHEN Inds.IndexType = 'J'
            THEN 'Join Index'
        WHEN Inds.IndexType = 'N'
        	THEN 'HASH Index'
        WHEN Inds.IndexType = 'V'
            THEN 'Value Ordered Secondary Index'
        WHEN Inds.IndexType = 'H'
            THEN 'Hash Ordered ALL (covering) Secondary Index'
        WHEN Inds.IndexType = 'O'
            THEN 'Value Ordered ALL (covering) Secondary Index'
        WHEN Inds.IndexType = 'I'
            THEN 'Ordering Column of a Composite Secondary Index'
        WHEN Inds.IndexType = 'M'
            THEN 'Multi-Column Statistics'
        WHEN Inds.IndexType = 'D'
            THEN 'Derived Column Partition Statistics'
        WHEN Inds.IndexType = 'G'
            THEN 'Geospatial nonunique secondary index'
        WHEN Inds.IndexType IS NULL
            THEN ''
        ELSE
            Inds.IndexType
     END AS IndexTypeDesc
    ,count(*) as Total
from DBC.IndicesV Inds
group by 1,2
) with data 
no primary index on commit preserve rows;
*/


/*{{temp:dim_tdinternal_databases.csv}}*/;


/*{{save:dat_dbobject_count_per_indextype.csv}}*/
select
   '{siteid}' as Site_ID 
  ,'Index Types' AS ReportName
  ,IndexTypeDesc
  ,SUM(IndexCount) AS Total
from index_types
Where DatabaseName NOT IN  (select dbname from "dim_tdinternal_databases.csv")      
group by 3;


/*{{save:dat_index_count_for_pptx.csv}}*/
select
   '{siteid}' as Site_ID 
  ,substring(indextypedesc from 1 for index(indextypedesc, '(') -1) AS IndexTypeDescription
  ,SUM(IndexCount) AS Total
from index_types
Where DatabaseName NOT IN  (select dbname from "dim_tdinternal_databases.csv")
  and IndexTypeDesc IN ('Unique Primary Index (UPI)', 'Partitioned Primary Index (unique)', 'Partitioned Primary Index (non-unique)')      
group by 2;

drop table "dim_tdinternal_databases.csv";
