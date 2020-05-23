/* builds the dim_user volatile table
   requires the dim_user.csv file

Parameters:
  - dbqlogtbl:    {dbqlogtbl}
  - startdate:    {startdate}
  - enddate:      {enddate}
*/


/*{{temp:dim_user.csv}}*/  ;
/*{{file:dim_user_override.sql}}*/;


create volatile table dim_user as
(
  select
   trim(o.UserName) as Username
  ,coalesce(p.User_Bucket,'Unknown') as User_Bucket
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
