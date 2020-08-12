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
