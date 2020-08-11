/* Pull all databases plus object counts, by type
   NOT by history, just point-in-time
   Parameters:
   - siteid:     {siteid}
   - spoolpct:   {spoolpct}  default 20%
*/

create volatile table db_object_counts as
(
    SELECT
    case when d.DatabaseName is null
        then '**** Totals ****'
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
        ,'' as CommentString
        ,sum( case when t.TableKind in('T','O','J','Q') then 1 else 0 end) as TableCount
        ,sum( case when t.TableKind in('V') then 1 else 0 end) as ViewCount
        ,sum( case when t.TableKind in('I','N') then 1 else 0 end) as IndexCount
        ,sum( case when t.TableKind in('M') then 1 else 0 end) as MacroCount
        ,sum( case when t.TableKind in('P','E','G') then 1 else 0 end) as "SP&TrigCount"
        ,sum( case when t.TableKind in('A','B','F','R','S','U','D') then 1 else 0 end) as UDObjectCount
        ,sum( case when t.TableKind in('H') then 1 else 0 end) as SysConstCount
        ,sum( case when t.TableKind NOT in('A','B','F','R','S','U','P','E','G','M','I','N','V','T','O','J','Q','D','H') then 1 else 0 end) as OtherCount
        FROM dbc.Tables t
        Group By 1,2
    ) d
    GROUP BY rollup (d.Databasename)
) with data no primary index on commit preserve rows
;


/*{{save:db_object_counts_all.csv}}*/
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
from db_object_counts
;

/*{{save:db_object_counts_total.csv}}*/
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
from db_object_counts
where DBName = '**** Totals ****'
;
