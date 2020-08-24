/* Pull all databases plus object counts, by type
   NOT by history, just point-in-time
   Parameters:
   - siteid:     {siteid}
   - spoolpct:   {spoolpct}  default 20%
*/

create volatile table db_objects_cds as
(
    SELECT
    Current_Date AS LogDate
    case when s.DatabaseName is null
        then '*** Total ***'
        else s.DatabaseName end as DBName
    ,cast( {spoolpct} as decimal(4,3)) as SpoolPct
    ,case when s.DatabaseName is null
          then '** Entire Teradata System (minus '||
          cast(cast(SpoolPct*100 as decimal(4,1) format'99.9') as char(4)) ||'% spool from MaxPerm) **'
        else '' end as CommentString
    ,cast(sum(MaxPerm)/1e9 as decimal(18,3))
      * case when s.DatabaseName is null then (1-SpoolPct) else 1.000 end as MaxPermGB
    ,ZeroIfNull(cast(NullifZero(sum(CurrentPerm))/1e9 as decimal(18,3))) as CurrentPermGB
    ,ZeroIfNull(CurrentPermGB/NullIfZero(MaxPermGB)) as FilledPct
    ,rank() over (order by CurrentPermGB desc) as CDSRank
    FROM
    (
        Select DatabaseName
        ,sum(MaxPerm) as MaxPerm
        ,sum(CurrentPerm) as CurrentPerm
        FROM dbc.diskspace
        Group By 1
    ) s
    GROUP BY rollup (s.Databasename)
) with data no primary index on commit preserve rows
;

                
/* build formatted return for pptx operations: */
create volatile table db_objects_cds_week_formatted as
(
  select
    '{siteid}' as Site_ID
   ,(cast(YearNumber_of_Calendar(LogDate,'ISO') as int)*1000) +
    (cast(MonthNumber_of_Year   (LogDate,'ISO') as int)*10) +
    (cast(WeekNumber_of_Month   (LogDate,'ISO') as int)) as "Week ID"
   ,DBName as "DB Name"
   ,SpoolPct as "Spool Pct"
   ,CommentString as "Comments"
   ,MaxPermGB as "MaxPerm GB"
   ,CurrentPermGB as "CurrPerm GB"
   ,FilledPct as "Fill Pct"
   ,rank() over(partition by "Week ID" order by "CurrPerm GB" desc)-1 as "CurrPerm Rank"
   from db_objects
 ) with data no primary index on commit preserve rows
 ;

                 
/*{{save:db_objects_cds-all.csv}}*/
Select * from db_objects_cds_week_formatted
;

/*{{save:db_objects_cds-total.csv}}*/
Select * from db_objects_cds_week_formatted
where "DB Name" = '*** Total ***'
;

/*{{save:db_objects_cds-top10.csv}}*/
Select * from db_objects_cds_week_formatted
where "DB Name" <> '*** Total ***'
and "CurrPerm Rank" <= 10
;

drop table db_objects_dates;
