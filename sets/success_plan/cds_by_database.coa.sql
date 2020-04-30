/* readout of CDS by database, with counts per sub-objects

Parameters:
  - siteid:     {siteid}
*/

/*{{temp:dim_tablekind.csv}}*/
/*{{save:cds_by_database.csv}}*/
Select
 '{siteid}' as SiteID
,DBName
,CommentString
,MaxPermGB
,CurrentPermGB
,FilledPct
,TableCount
,ViewCount
,IndexCount
,MacroCount
,"SP&TrigCount"
,ForeignServerCount
,UserDefinedCount
,FunctionCount
,OtherCount
,Current_Timestamp as RunTS
FROM (
  SELECT
     case when d.DatabaseName is null
          then '+=+= Totals =+=+'
          else d.DatabaseName end as DBName
    ,case when d.DatabaseName is null
          then '+=+= Entire Teradata System (minus 20% spool) =+=+'
          else Max(d.CommentString) end as CommentString
    ,cast(sum(MaxPerm)/1e9 as decimal(16,1))
     * case when DBName='+=+= Totals =+=+'  then 0.8 else 1 end as MaxPermGB
    ,ZeroIfNull(cast(NullifZero(sum(CurrentPerm))
      /1e9 as decimal(16,1))) as CurrentPermGB
    ,ZeroIfNull(cast(CurrentPermGB as decimal(16,4))
      /nullifzero(cast(MaxPermGB as decimal(16,4)))) as FilledPct
    ,Sum(d.TableCount) as TableCount
    ,Sum(d.ViewCount) as ViewCount
    ,Sum(d.IndexCount) as IndexCount
    ,Sum(d.MacroCount) as MacroCount
    ,Sum(d."SP&TrigCount") as "SP&TrigCount"
    ,Sum(d.ForeignServerCount) as ForeignServerCount
    ,Sum(d.UserDefinedCount) as UserDefinedCount
    ,Sum(d.FunctionCount) as FunctionCount
    ,Sum(d.OtherCount) as OtherCount
    FROM
    (
     Select DatabaseName
     ,sum(MaxPerm) as MaxPerm
     ,sum(CurrentPerm) as CurrentPerm
     FROM dbc.AllSpace
     WHERE TableName = 'All'
     GROUP By 1
    ) s
    JOIN
    (
      Select d.DatabaseName, d.CommentString
       ,sum( case when k.Table_Bucket in('Table') then 1 else 0 end) as TableCount
       ,sum( case when k.Table_Bucket in('View') then 1 else 0 end) as ViewCount
       ,sum( case when k.Table_Bucket in('Index') then 1 else 0 end) as IndexCount
       ,sum( case when k.Table_Bucket in('Macro') then 1 else 0 end) as MacroCount
       ,sum( case when k.Table_Bucket in('Stored Procedure','Trigger') then 1 else 0 end) as "SP&TrigCount"
       ,sum( case when k.Table_Bucket in('Foreign Server') then 1 else 0 end) as ForeignServerCount
       ,sum( case when k.Table_Bucket in('User Defined') then 1 else 0 end) as UserDefinedCount
       ,sum( case when k.Table_Bucket in('Function') then 1 else 0 end) as FunctionCount
       ,sum( case when k.Table_Bucket in('Other','Journal') then 1
                  when k.Table_Bucket is null then 1 else 0 end) as OtherCount
      FROM dbc.Databases as d
      LEFT OUTER JOIN dbc.Tables as t
        ON t.DatabaseName = d.DatabaseName
      LEFT OUTER JOIN "dim_tablekind.csv" as k
        ON t.TableKind = k.TableKind
      Group  By 1,2
 ) d
    ON s.DatabaseName = d.DatabaseName
    GROUP BY rollup (d.Databasename)
 ) d
