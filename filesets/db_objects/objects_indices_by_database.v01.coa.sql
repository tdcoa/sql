/* dimension  for index types
   Full outer join only shows when an index type does not exist at all, regardless of database.
   Database is only used as a possible filter in outer sets, per current requirements.

   Dependencies:
   -  dim_object
*/

create volatile table index_types_by_database as
(
 SEL
   COALESCE(Inds.DatabaseName, '') AS DatabaseName
  ,COALESCE(IK.IndexTypeDesc, 'Unknown - ' || Inds.IndexType) AS IndexTypeDesc
  ,ZEROIFNULL(Inds.IndexCount) AS IndexCount
  FROM
  (
    SEL
       DatabaseName
      ,IndexType
      ,UniqueFlag
      ,COUNT(*) AS IndexCount
    FROM DBC.IndicesV
    GROUP BY 1,2,3
  ) Inds
  FULL OUTER JOIN dim_indextype AS IK
   ON IK.IndexType = Inds.IndexType
  AND IK.UniqueFlag = Inds.UniqueFlag
) with data
no primary index on commit preserve rows;

/*{{save:dat_indextype_by_database.csv}}*/
Select * from index_types_by_database;
