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
    ,case when s.DatabaseName is null
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
   from db_objects_cds
 ) with data no primary index on commit preserve rows
 ;


 /*{{save:db_objects_permspace-all.csv}}*/
 /*{{load:{db_stg}.stg_dat_permspace_week}}*/
 /*{{call:{db_coa}.sp_dat_permspace_week('v1')}}*/
 Select '{siteid}' as Site_ID
 ,"Week ID" as Week_ID
 ,"DB Name" as DatabaseName
 ,"Spool Pct" as Spool_Pct
 ,"Comments" as CommentString
 ,"MaxPerm GB" as MaxPerm_GB
 ,"CurrPerm GB" as CurrPerm_GB
 ,"Fill Pct" as Fill_Pct
 ,"CurrPerm Rank" as CurrPerm_Rank
 from db_objects_cds_week_formatted as cds
 ;

 /*{{save:db_objects_permspace-total.csv}}*/
 Select '{siteid}' as Site_ID, cds.*
 from db_objects_cds_week_formatted as cds
 where "DB Name" = '*** Total ***'
 ;

 /*{{save:db_objects_permspace_top10.csv}}*/
 Select '{siteid}' as Site_ID, cds.*
 from db_objects_cds_week_formatted as cds
 where "DB Name" <> '*** Total ***'
 qualify "CurrPerm Rank" <= 10
     and "Week ID" = Max("Week ID") over()
 ;
