SELECT
 case when d.logdate is null 
    then date
    else d.logdate end as log_date
,case when WeekNumber_Of_Year(d.logdate) is null
    then '**** Totals ****'
    else WeekNumber_Of_Year(d.logdate) end as week_id
,case when d.DatabaseName is null
    then '**** Totals ****'
    else d.DatabaseName end as DBName
,case when d.DatabaseName is null
    then '** Entire Teradata System (minus 20% spool) **'
    else Max(d.CommentString) end as CommentString
,cast(sum(MaxPerm)/1e9 as decimal(18,3))
  * case when d.DatabaseName is null then 0.800 else 1.000 end as MaxPermGB
,ZeroIfNull(cast(NullifZero(sum(CurrentPerm))/1e9 as decimal(18,3))) as CurrentPermGB
,CurrentPermGB/NullIfZero(MaxPermGB) as FilledPct
,Sum(d.TableCount) as TableCount
,Sum(d.ViewCount) as ViewCount
,Sum(d.IndexCount) as IndexCount
,Sum(d.MacroCount) as MacroCount
,Sum(d."SP&TrigCount") as "SP&TrigCount"
,Sum(d.UDObjectCount) as UDObjectCount
,Sum(d.OtherCount) as OtherCount
FROM
(
    Select DatabaseName
	,logdate
    ,sum(MaxPerm) as MaxPerm
    ,sum(CurrentPerm) as CurrentPerm
    FROM PDCRDATA.DatabaseSpace_Hst
	where logdate = date - 30
    Group By 1,2
) s
JOIN
(
    Select t.DatabaseName, '' as CommentString
	,logdate 
    ,sum( case when t.TableKind in('T','O','J','Q') then 1 else 0 end) as TableCount
    ,sum( case when t.TableKind in('V') then 1 else 0 end) as ViewCount
    ,sum( case when t.TableKind in('I','N') then 1 else 0 end) as IndexCount
    ,sum( case when t.TableKind in('M') then 1 else 0 end) as MacroCount
    ,sum( case when t.TableKind in('P','E','G') then 1 else 0 end) as "SP&TrigCount"
    ,sum( case when t.TableKind in('A','B','F','R','S','U','D') then 1 else 0 end) as UDObjectCount
    ,sum( case when t.TableKind in('H') then 1 else 0 end) as SysConstCount
    ,sum( case when t.TableKind NOT in('A','B','F','R','S','U','P','E','G','M','I','N','V','T','O','J','Q','D','H') then 1 else 0 end) as OtherCount
    FROM dbc.Tables t
	inner join PDCRDATA.TableSpace_Hst h
	on h.Tablename = t.Tablename
	and h.DatabaseName = t.DatabaseName
    Group By 1,2,3
) d
ON s.DatabaseName = d.DatabaseName
and s.logdate = d.logdate

GROUP BY rollup (d.Databasename,d.logdate)
order by 1