/* prework for DBQL_Core and Concurrency Queries

Parameters:
  - startdate:    {startdate}
  - enddate:      {enddate}
  - siteid:       {siteid}
  - dbqlogtbl:    {dbqlogtbl}
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
  ,coalesce(p.SiteID, 'None')        as SiteID_
  from (select distinct AppID from {dbqlogtbl}
        where LogDate between {startdate} and {enddate}) as o
  left join "dim_app.csv" as p
    on (case
        when p.Pattern_Type = 'Equal' and o.AppID = p.Pattern then 1
        when p.Pattern_Type = 'Like'  and o.AppID like p.Pattern then 1
        when p.Pattern_Type = 'RegEx'
         and character_length(regexp_substr(o.AppID, p.Pattern,1,1,'i'))>0 then 1
        else 0 end) = 1
  qualify Priority_ = min(Priority_)over(partition by o.AppID)
  where SiteID_ in('default','None') or SiteID_ like '{siteid}'
) with data
no primary index
on commit preserve rows;

drop table "dim_app.csv";

/*{{save:dim_app_reconcile.csv}}*/
Select * from dim_App
order by case when  App_Bucket='Unknown' then '!!!' else App_Bucket end asc
;



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
  from (select distinct StatementType from {dbqlogtbl}
        where LogDate between {startdate} and {enddate}) as o
  left join "dim_statement.csv"  as p
    on (case
        when p.Pattern_Type = 'Equal' and o.StatementType = p.Pattern then 1
        when p.Pattern_Type = 'Like'  and o.StatementType like p.Pattern then 1
        when p.Pattern_Type = 'RegEx'
         and character_length(regexp_substr(o.StatementType, p.Pattern,1,1,'i'))>0 then 1
        else 0 end) = 1
  qualify Priority_ = min(Priority_)over(partition by o.StatementType)
  where SiteID_ in('default','None') or SiteID_ like '{siteid}'
) with data
no primary index
on commit preserve rows;

drop table "dim_statement.csv";

/*{{save:dim_statement_reconcile.csv}}*/
Select * from dim_statement
order by case when  Statement_Bucket='Unknown' then '!!!' else Statement_Bucket end asc
;



/*{{temp:dim_user.csv}}*/
create volatile table dim_user as
(
  select
   trim(o.UserName) as Username
  ,coalesce(p.User_Bucket,'Unknown') as User_Bucket
  ,coalesce(p.Is_Discrete_Human,'Unk') as Is_Discrete_Human
  ,coalesce(p.User_Department, 'Unknown') as User_Department
  ,coalesce(p.User_SubDepartment, 'Unknown') as User_SubDepartment
  ,coalesce(p.User_Region, 'Unknown') as User_Region
  ,coalesce(p.Priority,1e6) as Priority_
  ,coalesce(p.Pattern_Type,'Equal')  as Pattern_Type
  ,coalesce(p.Pattern, o.UserName) as Pattern
  ,coalesce(p.SiteID, 'None')        as SiteID_
  from (select distinct UserName from {dbqlogtbl}
        where LogDate between {startdate} and {enddate}) as o
  left join "dim_user.csv" as p
    on (case
        when p.Pattern_Type = 'Equal' and o.UserName = p.Pattern then 1
        when p.Pattern_Type = 'Like'  and o.UserName like p.Pattern then 1
        when p.Pattern_Type = 'RegEx'
         and character_length(regexp_substr(o.UserName, p.Pattern,1,1,'i'))>0 then 1
        else 0 end) = 1
  qualify Priority_ = min(Priority_)over(partition by o.UserName)
  where SiteID_ in('default','None') or SiteID_ like '{siteid}'
) with data
no primary index
on commit preserve rows;

drop table "dim_user.csv"
;



/* below override sql file allows opportunity to
   replace dim_user.csv with ca_user_xref table
   or a customer-specific table.  To use, review
   and fill-in the .sql file content:
*/
/*{{file:override_user_dim.sql}}*/
;

/*{{save:dim_user_reconcile.csv}}*/
Select hashrow(substr(Username,1,1))                                     /* first character  */
    || hashrow(substr(Username,floor(character_length(Username)/2)+1,1)) /* middle character */
    || hashrow(substr(Username,character_length(Username),1))            /* last character   */
    || hashrow(Username)                                                 /* entire value */
        as hash_userid
,usr.*
from dim_user as usr
order by case when User_Bucket='Unknown' then '!!!' else User_Bucket end asc
;
