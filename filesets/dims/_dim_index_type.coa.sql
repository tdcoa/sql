/* dimension  for index types

*/



/*{{temp:dim_indexkind.csv}}*/;

/*{{temp:dim_tdinternal_databases.csv}}*/;



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




/*{{save:dat_dbobject_count_per_indextype.csv}}*/

select

   '{siteid}' as Site_ID 

  ,'Index Types' AS ReportName

  ,IndexTypeDesc

  ,SUM(IndexCount) AS Total

from index_types

Where DatabaseName NOT IN  (select dbname from "dim_tdinternal_databases.csv")      

group by 3;



drop table "dim_tdinternal_databases.csv";
