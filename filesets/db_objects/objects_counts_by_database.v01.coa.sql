/* Pull all databases plus object counts, by type
   NOT by history, just point-in-time
   Parameters:
   - siteid:     {siteid}
*/

/*{{temp:dim_tablekind.csv}}*/
create volatile table db_objects_counts as
(
    SELECT
     Current_Date as LogDate
    ,case when d.DatabaseName is null
        then '*** Total ***'
        else d.DatabaseName end as DBName
    ,sum(d.TableCount) as TableCount
    ,sum(d.ViewCount) as ViewCount
    ,sum(d.IndexCount) as IndexCount
    ,sum(d.MacroCount) as MacroCount
    ,sum(d."SP&TrigCount") as "SP&TrigCount"
    ,sum(d.UDObjectCount) as UDObjectCount
    ,sum(d.OtherCount) as OtherCount
    FROM
    (
     Select t.DatabaseName
    ,sum( case when tk.Table_Bucket = 'Table' then 1 else 0 end) as TableCount
    ,sum( case when tk.Table_Bucket = 'View' then 1 else 0 end) as ViewCount
    ,sum( case when tk.Table_Bucket = 'Index' then 1 else 0 end) as IndexCount
    ,sum( case when tk.Table_Bucket = 'Macro' then 1 else 0 end) as MacroCount
    ,sum( case when tk.Table_Bucket in('Stored Procedure','Trigger') then 1 else 0 end) as "SP&TrigCount"
    ,sum( case when tk.Table_Bucket = 'Function' then 1 else 0 end) as FunctionCount
    ,sum( case when tk.Table_Bucket = 'User Defined' then 1 else 0 end) as UDObjectCount
    ,sum( case when tk.Table_Bucket = 'Foreign Server' then 1 else 0 end) as ForeignServerCount
    ,sum( case when tk.Table_Bucket in('Other','Journal')
                 or tk.Table_Bucket is null then 1 else 0 end) as OtherCount
    FROM dbc.Tables t
    LEFT OUTER JOIN "dim_tablekind.csv" tk
    on t.TableKind = tk.TableKind
    group by 1
    ) d
    GROUP BY rollup (d.Databasename)
) with data no primary index on commit preserve rows
;


/*{{save:dat_objectkind_count-all.csv}}*/
Select
 '{siteid}' as Site_ID
,DBName
,cast(cast(TableCount     as integer       format'ZZZ,ZZZ,ZZZ,ZZZ')    as varchar(32)) as Table_Count
,cast(cast(ViewCount      as integer       format'ZZZ,ZZZ,ZZZ,ZZZ')    as varchar(32)) as View_Count
,cast(cast(IndexCount     as integer       format'ZZZ,ZZZ,ZZZ,ZZZ')    as varchar(32)) as Index_Count
,cast(cast(MacroCount     as integer       format'ZZZ,ZZZ,ZZZ,ZZZ')    as varchar(32)) as Macro_Count
,cast(cast("SP&TrigCount" as integer       format'ZZZ,ZZZ,ZZZ,ZZZ')    as varchar(32)) as SPTrig_Count
,cast(cast(UDObjectCount  as integer       format'ZZZ,ZZZ,ZZZ,ZZZ')    as varchar(32)) as UDObject_Count
,cast(cast(OtherCount     as integer       format'ZZZ,ZZZ,ZZZ,ZZZ')    as varchar(32)) as Other_Count
from db_objects_counts
;

/*{{save:dat_objectkind_count-total.csv}}*/
Select
 '{siteid}' as Site_ID
,DBName as "Database Name"
,cast(cast(zeroifnull(TableCount    ) as integer       format'ZZZ,ZZZ,ZZZ,ZZ9')    as varchar(32)) as "Table Count"
,cast(cast(zeroifnull(ViewCount     ) as integer       format'ZZZ,ZZZ,ZZZ,ZZ9')    as varchar(32)) as "View Count"
,cast(cast(zeroifnull(IndexCount    ) as integer       format'ZZZ,ZZZ,ZZZ,ZZ9')    as varchar(32)) as "Index Count"
,cast(cast(zeroifnull(MacroCount    ) as integer       format'ZZZ,ZZZ,ZZZ,ZZ9')    as varchar(32)) as "Macro Count"
,cast(cast(zeroifnull("SP&TrigCount") as integer       format'ZZZ,ZZZ,ZZZ,ZZ9')    as varchar(32)) as "SP&Trig Count"
,cast(cast(zeroifnull(UDObjectCount ) as integer       format'ZZZ,ZZZ,ZZZ,ZZ9')    as varchar(32)) as "UDObject Count"
,cast(cast(zeroifnull(OtherCount    ) as integer       format'ZZZ,ZZZ,ZZZ,ZZ9')    as varchar(32)) as "Other Count"
from db_objects_counts
where DBName = '*** Total ***'
;
