/* Pull all databases space
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
   (cast(WeekNumber_of_Month   (calendar_date,'ISO') as int)) as Week_ID
  from sys_calendar.calendar
  where Week_of_Calendar in
      (Select week_of_calendar from sys_calendar.calendar
       where calendar_date between {startdate} and {enddate})  /*  PDCR picks first day of every week */
    and Day_of_Week = {day_of_week}
) with data no primary index on commit preserve rows
;

collect stats on db_objects_dates column(Week_ID)
;


create volatile table db_objects_cds as
(
  SELECT
   dt.Week_ID
  ,case when s.DatabaseName is null
      then '**** Totals ****'
      else s.DatabaseName end as DBName
  ,cast( {spoolpct} as decimal(4,3)) as SpoolPct
  ,case when s.DatabaseName is null
        then '** Entire Teradata System (minus '||
        cast(cast(SpoolPct*100 as decimal(4,1) format'99.9') as char(4)) ||'% spool from MaxPerm) **'
      else '' end as CommentString
  ,cast(avg(MaxPerm)/1e9 as decimal(18,3))
    * case when s.DatabaseName is null then (1-SpoolPct) else 1.000 end as MaxPermGB
  ,ZeroIfNull(cast(NullifZero(avg(CurrentPerm))/1e9 as decimal(18,3))) as CurrentPermGB
  ,ZeroIfNull(CurrentPermGB/NullIfZero(MaxPermGB)) as FilledPct
  ,rank() over (partition by dt.Week_ID order by dt.Week_ID, CurrentPermGB desc) as CDSRank
  FROM
  (
      Select 
       DatabaseName
  	   ,logdate
      ,sum(MaxPerm) as MaxPerm
      ,sum(CurrentPerm) as CurrentPerm
      FROM PDCRDATA.DatabaseSpace_Hst
  	   where logdate in (select LogDate from db_objects_dates)
      Group By 1,2
  ) s
  JOIN db_objects_dates as dt
    on s.LogDate = dt.LogDate
   and d.LogDate = dt.LogDate
  GROUP BY dt.Week_ID, rollup(s.Databasename)

) with data no primary index on commit preserve rows
;


/*{{save:db_objects_cds_all.csv}}*/
Select
 '{siteid}' as Site_ID
,Week_ID
,DBName
,rank() over(order by CurrentPermGB desc)-1 as CurrPermGB_Rank
,CommentString
,cast(cast(MaxPermGB      as decimal(18,2) format'ZZZ,ZZZ,ZZZ,ZZ9.99') as varchar(32)) as MaxPermGB
,cast(cast(CurrentPermGB  as decimal(18,2) format'ZZZ,ZZZ,ZZZ,ZZ9.99') as varchar(32)) as CurrentPermGB
,cast(cast(case when FilledPct >1 then 101.0 else FilledPct*100 end
                          as decimal( 9,3) format'ZZZ,Z99.999')        as varchar(32)) as FilledPct
from db_objects_cds
;

/*{{save:db_objects_cds_total.csv}}*/
Select
 '{siteid}' as Site_ID
,Week_ID
,DBName as "Database Name"
,SpoolPct as "Spool%"
,CommentString as "Comment String"
,cast(cast(zeroifnull(MaxPermGB     ) as decimal(18,2) format'ZZZ,ZZZ,ZZZ,ZZ9.99') as varchar(32)) as "Allocated GB"
,cast(cast(zeroifnull(CurrentPermGB ) as decimal(18,2) format'ZZZ,ZZZ,ZZZ,ZZ9.99') as varchar(32)) as "Used GB"
,cast(cast(zeroifnull(case when FilledPct >1 then 101.0 else FilledPct*100 end )
                                      as decimal( 9,3) format'ZZZ,Z99.999')        as varchar(32)) as "Filled Pct"
,rank() over(order by Week_ID desc) as "Used GB Rank"
from db_objects_cds
where DBName = '**** Totals ****'
;

/*{{save:db_objects_cds_top10.csv}}*/
Select
'{siteid}' as Site_ID
,Week_ID
,DBName as "Database Name"
,rank() over(partition by Week_ID order by CurrentPermGB desc) as "Used GB Rank"
,CommentString as "Comment String"
,cast(cast(zeroifnull(MaxPermGB     ) as decimal(18,2) format'ZZZ,ZZZ,ZZZ,ZZ9.99') as varchar(32)) as "Allocated GB"
,cast(cast(zeroifnull(CurrentPermGB ) as decimal(18,2) format'ZZZ,ZZZ,ZZZ,ZZ9.99') as varchar(32)) as "Used GB"
,cast(cast(zeroifnull(case when FilledPct >1 then 101.0 else FilledPct*100 end )
                                      as decimal( 9,3) format'ZZZ,Z99.999')        as varchar(32)) as "Filled Pct"
from db_objects_cds
where DBName <> '**** Totals ****'
qualify "Used GB Rank" <= 10
    and max(Week_ID) over()  = Week_ID
;


drop table db_objects_dates;
