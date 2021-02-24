/* Start COA: Dim_Statement
   builds the dim_statement volatile table
   requires the dim_statement.csv file

Parameters:
  - dbqlogtbl:    {dbqlogtbl}
  - startdate:    {startdate}
  - enddate:      {enddate}
*/


/*{{temp:dim_statement.csv}}*/
create volatile table dim_statement as
(
  select
   o.StatementType
  ,coalesce(p.Statement_Bucket,'Unknown') as Statement_Bucket
  ,coalesce(p.Priority,1e6) as Priority_
  ,coalesce(p.Pattern_Type,'Equal')  as Pattern_Type
  ,coalesce(p.Pattern, o.StatementType) as Pattern
  ,coalesce(p.SiteID, 'None')        as SiteID_
  from (select distinct StatementType from pdcrinfo.DBQLogTbl_Hst
        where CAST(StartTime AS DATE) between {startdate} and {enddate}
        union all
        select distinct 'Summary' from pdcrinfo.DBQLSummaryTbl_Hst 
        where CAST(CollectTimeStamp AS DATE) between {startdate} and {enddate}) as o
  left join "dim_statement.csv"  as p
    on (case
        when p.Pattern_Type = 'Equal' and lower(o.StatementType) = lower(p.Pattern) then 1
        when p.Pattern_Type = 'Like'  and lower(o.StatementType) like lower(p.Pattern) then 1
        when p.Pattern_Type = 'RegEx'
         and character_length(regexp_substr(o.StatementType, p.Pattern,1,1,'i'))>0 then 1
        else 0 end) = 1
  qualify Priority_ = min(Priority_)over(partition by o.StatementType)
  where (lower(SiteID_) in('default','none') or lower('{siteid}') like lower(SiteID_))
) with data
no primary index
on commit preserve rows;

drop table "dim_statement.csv";

/*{{save:dim_statement_reconcile.csv}}*/
Select * from dim_statement
order by case when  Statement_Bucket='Unknown' then '!!!' else Statement_Bucket end asc
;

/* End COA: Dim_Statement */
