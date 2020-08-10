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
group by 1,2
) with data 
no primary index on commit preserve rows;


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
/*{{pptx:snowflake_migration_blockers.pptx}}*/
select
   '{siteid}' as Site_ID 
  ,substring(indextypedesc from 1 for index(indextypedesc, '(') -1) AS IndexTypeDescription
  ,SUM(IndexCount) AS Total
from index_types
Where DatabaseName NOT IN  (select dbname from "dim_tdinternal_databases.csv")
  and IndexTypeDesc IN ('Unique Primary Index (UPI)', 'Partitioned Primary Index (unique)', 'Partitioned Primary Index (non-unique)')      
group by 2
order by 2;

drop table "dim_tdinternal_databases.csv";

