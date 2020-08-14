/* dimension  for table kinds
*//*{{temp:dim_tablekind.csv}}*/;

create volatile table table_kinds_by_database as 
(SELECT     Current_Date AS LogDate    ,DatabaseName    ,Table_Bucket    ,TableKind_Desc    ,CheckOpt AS MultisetInd                         ,COUNT(*) AS ObjectCountFROM DBC.TablesV as  tJOIN "dim_tablekind.csv" as tk  on t.TableKind = tk.TableKind
GROUP BY 2,3,4,5
) with data 
no primary index on commit preserve rows;
