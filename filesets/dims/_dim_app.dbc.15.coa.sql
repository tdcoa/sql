/* Start COA: Dim_App
   builds the dim_app volatile table
   requires the dim_app.csv file

Parameters:
  - dbqlogtbl:    {dbqlogtbl}
  - startdate:    {startdate}
  - enddate:      {enddate}
*/


/*{{temp:dim_app.csv}}*/
create volatile table dim_app as
(
  select
   o.AppID
  ,coalesce(p.App_Bucket,'Unknown') as App_Bucket
  ,coalesce(p.Use_Bucket,'Unknown')  as Use_Bucket
  ,coalesce(p.Priority,1e6) as Priority_
  ,coalesce(p.Pattern_Type,'Equal')  as Pattern_Type
  ,coalesce(p.Pattern, o.AppID)      as Pattern
  ,coalesce(o.Request_Cnt, 0)        as Request_Cnt
  ,coalesce(p.SiteID, 'None')        as SiteID_
  from (Select AppID, sum(Request_Cnt) as Request_Cnt
        from(
                select AppID, count(*) as Request_Cnt from DBC.QryLogV
                where cast(StartTime as date) between {startdate} and {enddate}
                group by 1
                union
                select AppID, count(*) as Request_Cnt from DBC.QryLogSummaryV
                where cast(CollectTimeStamp as date) between {startdate} and {enddate}
                group by 1
        ) as a
        group by 1
      ) as o
  left join "dim_app.csv" as p
    on (case
        when p.Pattern_Type = 'Equal' and lower(o.AppID) = lower(p.Pattern) then 1
        when p.Pattern_Type = 'Like'  and lower(o.AppID) like lower(p.Pattern) then 1
        else 0 end) = 1
  qualify Priority_ = min(Priority_)over(partition by o.AppID)
  where (lower(SiteID_) in('default','none') or lower('{siteid}') like lower(SiteID_))
) with data
no primary index
on commit preserve rows;

drop table "dim_app.csv";

/*{  removed {save:dim_app_reconcile.csv}}
Select * from dim_App
order by case when  App_Bucket='Unknown' then '!!!' else App_Bucket end asc
*/

/* End COA: Dim_App */
