/* dimension  for table kinds
   Full outer join only shows when an table kind does not exist at all, regardless of database.
   Database is only used as a filter in outer scripts.
*/


create volatile table table_kinds_by_database as 
(
  SELECT
     COALESCE(DatabaseName, '') AS DatabaseName
    ,COALESCE(tk.Table_Bucket, 'Unknown - ' || t.TableKind) AS TableBucket
    ,COALESCE(tk.TableKind_Desc, 'Unknown - ' || t.TableKind) AS TableKindDesc
    ,CheckOpt AS MultisetInd                     
    ,ZEROIFNULL(ObjectCount) AS ObjectCount
  FROM
  (
   SELECT
     DatabaseName
    ,TableKind 
    ,CheckOpt                     
    ,COUNT(*) AS ObjectCount
   FROM DBC.TablesV 
   GROUP BY 1,2,3
  ) t
  FULL OUTER JOIN "dim_tablekind.csv" as tk
  on t.TableKind = tk.TableKind
) with data 
no primary index on commit preserve rows;
