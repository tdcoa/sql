/* Pull all databases plus object counts, by type
   NOT by history, just point-in-time

   Parameters:
   - siteid:     {siteid}
   - spoolpct:   {spoolpct}  default 20%
   - day_of_week: {day_of_week}   default 1


*/

create volatile table db_objects_dates as
(
  select Calendar_Date as LogDate
  ,(cast(YearNumber_of_Calendar(calendar_date,'ISO') as int)*1000) +
   (cast(MonthNumber_of_Year   (calendar_date,'ISO') as int)*10) +
   (cast(WeekNumber_of_Month   (calendar_date,'ISO') as int)) as WeekID
  from sys_calendar.calendar
  where Week_of_Calendar in
      (Select week_of_calendar from sys_calendar.calendar
       where calendar_date between {startdate} and {enddate})  /*  PDCR picks first day of every week */
    and Day_of_Week = {day_of_week}
) with data no primary index on commit preserve rows
;


create volatile table db_objects as
(
  SELECT
   dt.WeekID
  ,case when d.DatabaseName is null
      then '**** Totals ****'
      else d.DatabaseName end as DBName
  ,cast( {spoolpct} as decimal(4,3)) as SpoolPct
  ,case when d.DatabaseName is null
        then '** Entire Teradata System (minus '||
        cast(cast(SpoolPct*100 as decimal(4,1) format'99.9') as char(4)) ||'% spool from MaxPerm) **'
      else Max(d.CommentString) end as CommentString
  ,cast(sum(MaxPerm)/1e9 as decimal(18,3))
    * case when d.DatabaseName is null then (1-SpoolPct) else 1.000 end as MaxPermGB
  ,ZeroIfNull(cast(NullifZero(sum(CurrentPerm))/1e9 as decimal(18,3))) as CurrentPermGB
  ,ZeroIfNull(CurrentPermGB/NullIfZero(MaxPermGB)) as FilledPct
  ,Sum(d.TableCount) as TableCount
  ,Sum(d.ViewCount) as ViewCount
  ,Sum(d.IndexCount) as IndexCount
  ,Sum(d.MacroCount) as MacroCount
  ,Sum(d."SP&TrigCount") as "SP&TrigCount"
  ,Sum(d.UDObjectCount) as UDObjectCount
  ,Sum(d.OtherCount) as OtherCount
  ,rank() over (partition by dt.WeekID order by dt.WeekID, CurrentPermGB desc) as CDSRank
  FROM
  (
      Select DatabaseName
  	,logdate
      ,sum(MaxPerm) as MaxPerm
      ,sum(CurrentPerm) as CurrentPerm
      FROM PDCRDATA.DatabaseSpace_Hst
  	where logdate in (select LogDate from db_objects_dates)
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
      where h.logdate in (select LogDate from db_objects_dates)
      Group By 1,2,3
  ) d
  ON s.DatabaseName = d.DatabaseName
  and s.logdate = d.logdate
  JOIN db_objects_dates as dt
    on s.LogDate = dt.LogDate
   and d.LogDate = dt.LogDate
  GROUP BY dt.WeekID, rollup(d.Databasename)

) with data no primary index on commit preserve rows
;


/*{{save:db_objects_all.csv}}*/
/*{{load:{db_stg}.stg_dat_DB_Objects}}*/
/*{{call:{db_coa}.sp_dat_DB_Objects('{fileset_version}')}}*/
Select
 '{siteid}' as Site_ID
,WeekID
,DBName
,rank() over(order by CurrentPermGB desc)-1 as CurrPermGB_Rank
,CommentString
,cast(cast(MaxPermGB      as decimal(18,2) format'ZZZ,ZZZ,ZZZ,ZZ9.99') as varchar(32)) as MaxPermGB
,cast(cast(CurrentPermGB  as decimal(18,2) format'ZZZ,ZZZ,ZZZ,ZZ9.99') as varchar(32)) as CurrentPermGB
,cast(cast(FilledPct*100  as decimal( 9,3) format'999.99')             as varchar(32)) as FilledPct
,cast(cast(TableCount     as integer       format'ZZZ,ZZZ,ZZZ,ZZZ')    as varchar(32)) as Table_Count
,cast(cast(ViewCount      as integer       format'ZZZ,ZZZ,ZZZ,ZZZ')    as varchar(32)) as View_Count
,cast(cast(IndexCount     as integer       format'ZZZ,ZZZ,ZZZ,ZZZ')    as varchar(32)) as Index_Count
,cast(cast(MacroCount     as integer       format'ZZZ,ZZZ,ZZZ,ZZZ')    as varchar(32)) as Macro_Count
,cast(cast("SP&TrigCount" as integer       format'ZZZ,ZZZ,ZZZ,ZZZ')    as varchar(32)) as SPTrig_Count
,cast(cast(UDObjectCount  as integer       format'ZZZ,ZZZ,ZZZ,ZZZ')    as varchar(32)) as UDObject_Count
,cast(cast(OtherCount     as integer       format'ZZZ,ZZZ,ZZZ,ZZZ')    as varchar(32)) as Other_Count
from db_objects
;

/*{{save:db_objects_total.csv}}*/
Select
 '{siteid}' as Site_ID
,WeekID
,DBName as "Database Name"
,SpoolPct as "Spool%"
,CommentString as "Comment String"
,cast(cast(zeroifnull(MaxPermGB     ) as decimal(18,2) format'ZZZ,ZZZ,ZZZ,ZZ9.99') as varchar(32)) as "Allocated GB"
,cast(cast(zeroifnull(CurrentPermGB ) as decimal(18,2) format'ZZZ,ZZZ,ZZZ,ZZ9.99') as varchar(32)) as "Used GB"
,cast(cast(zeroifnull(FilledPct*100 ) as decimal( 9,3) format'999.99')             as varchar(32)) as "Filled Pct"
,cast(cast(zeroifnull(TableCount    ) as integer       format'ZZZ,ZZZ,ZZZ,ZZ9')    as varchar(32)) as "Table Count"
,cast(cast(zeroifnull(ViewCount     ) as integer       format'ZZZ,ZZZ,ZZZ,ZZ9')    as varchar(32)) as "View Count"
,cast(cast(zeroifnull(IndexCount    ) as integer       format'ZZZ,ZZZ,ZZZ,ZZ9')    as varchar(32)) as "Index Count"
,cast(cast(zeroifnull(MacroCount    ) as integer       format'ZZZ,ZZZ,ZZZ,ZZ9')    as varchar(32)) as "Macro Count"
,cast(cast(zeroifnull("SP&TrigCount") as integer       format'ZZZ,ZZZ,ZZZ,ZZ9')    as varchar(32)) as "SP&Trig Count"
,cast(cast(zeroifnull(UDObjectCount ) as integer       format'ZZZ,ZZZ,ZZZ,ZZ9')    as varchar(32)) as "UDObject Count"
,cast(cast(zeroifnull(OtherCount    ) as integer       format'ZZZ,ZZZ,ZZZ,ZZ9')    as varchar(32)) as "Other Count"
from db_objects
where DBName = '**** Totals ****'
;

/*{{save:db_objects_top10.csv}}*/
Select
'{siteid}' as Site_ID
,WeekID
,DBName as "Database Name"
,rank() over(order by CurrentPermGB desc) as "Used GB Rank"
,CommentString as "Comment String"
,cast(cast(zeroifnull(MaxPermGB     ) as decimal(18,2) format'ZZZ,ZZZ,ZZZ,ZZ9.99') as varchar(32)) as "Allocated GB"
,cast(cast(zeroifnull(CurrentPermGB ) as decimal(18,2) format'ZZZ,ZZZ,ZZZ,ZZ9.99') as varchar(32)) as "Used GB"
,cast(cast(zeroifnull(FilledPct*100 ) as decimal( 9,3) format'999.99')             as varchar(32)) as "Filled Pct"
,cast(cast(zeroifnull(TableCount    ) as integer       format'ZZZ,ZZZ,ZZZ,ZZ9')    as varchar(32)) as "Table Count"
,cast(cast(zeroifnull(ViewCount     ) as integer       format'ZZZ,ZZZ,ZZZ,ZZ9')    as varchar(32)) as "View Count"
,cast(cast(zeroifnull(IndexCount    ) as integer       format'ZZZ,ZZZ,ZZZ,ZZ9')    as varchar(32)) as "Index Count"
,cast(cast(zeroifnull(MacroCount    ) as integer       format'ZZZ,ZZZ,ZZZ,ZZ9')    as varchar(32)) as "Macro Count"
,cast(cast(zeroifnull("SP&TrigCount") as integer       format'ZZZ,ZZZ,ZZZ,ZZ9')    as varchar(32)) as "SP&Trig Count"
,cast(cast(zeroifnull(UDObjectCount ) as integer       format'ZZZ,ZZZ,ZZZ,ZZ9')    as varchar(32)) as "UDObject Count"
,cast(cast(zeroifnull(OtherCount    ) as integer       format'ZZZ,ZZZ,ZZZ,ZZ9')    as varchar(32)) as "Other Count"
from db_objects
where DBName <> '**** Totals ****'
qualify "Used GB Rank" <= 10
;
