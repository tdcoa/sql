/* Start COA: Dim_User
   builds the dim_user volatile table
   requires the dim_user.csv file

Parameters:
  - siteid:    {siteid}
*/


/* below override sql file allows opportunity to
   replace dim_user.csv with ca_user_xref table
   or a customer-specific table.  To use, review
   and fill-in the .sql file content:
*/

/*{{temp:dim_tdinternal_databases.csv}}*/ ;
/*{{temp:dim_user.csv}}*/ ;

/* insert the list of internal databases into dim_user
   --> if you use dim_user_override, be aware this happens BEFORE your override
       i.e. don't delete these records if you want them
*/
insert into "dim_user.csv"
  'Default'     as Site_ID
  ,'Equal'      as Pattern_Type,
  ,trim(dbname) as Pattern
  ,'TDInternal' as User_Bucket
  ,'TDInternal' as User_Department
  ,'TDInternal' as User_SubDepartment
  ,'TDInternal' as User_Region
  ,1            as Priority
from "dim_tdinternal_databases.csv" idb;

/*{{file:dim_user_override.sql}}*/ ;

create volatile table dim_user as
(
  select
   o.UserName
  ,o.UserHash
  ,coalesce(p.User_Bucket,'Unknown') as User_Bucket
  ,coalesce(p.User_Department, 'Unknown') as User_Department
  ,coalesce(p.User_SubDepartment, 'Unknown') as User_SubDepartment
  ,coalesce(p.User_Region, 'Unknown') as User_Region
  ,coalesce(p.Priority,1e6) as Priority_
  ,coalesce(p.Pattern_Type,'Equal')  as Pattern_Type
  ,coalesce(p.Pattern, o.UserName) as Pattern
  ,coalesce(p.SiteID, 'None')        as SiteID_
  from (select
         trim(DatabaseName) as UserName
        ,substr(Username,1,3) as first3
        ,substr(Username,floor(character_length(Username)/2)-1,3) as middle3
        ,substr(Username,character_length(Username)-3,3) as last3
        /* generate UserHash value */
        ,trim(cast(from_bytes(hashrow( Username),'base16') as char(9))) ||
         trim(cast(from_bytes(hashrow( first3  ),'base16') as char(9))) ||
         trim(cast(from_bytes(hashrow( middle3 ),'base16') as char(9))) ||
         trim(cast(from_bytes(hashrow( last3   ),'base16') as char(9))) as UserHash
        from dbc.DatabasesV where DBKind = 'U'
        ) as o
  left join "dim_user.csv" as p
    on (case
        when p.Pattern_Type = 'Equal' and lower(o.UserName) = lower(p.Pattern) then 1
        when p.Pattern_Type = 'Like'  and lower(o.UserName) like lower(p.Pattern) then 1
        when p.Pattern_Type = 'RegEx'
         and character_length(regexp_substr(o.UserName, p.Pattern,1,1,'i'))>0 then 1
        else 0 end) = 1
    and (lower(SiteID_) in('default','none') or lower('{siteid}') like lower(SiteID_))
  qualify Priority_ = min(Priority_)over(partition by o.UserName)
) with data
primary index (UserName)
on commit preserve rows
;

drop table "dim_user.csv"
;

collect stats on dim_user column(UserName)
;

/*{{save:dim_user_reconcile.csv}}*/
Select
 '{siteid}' as Site_ID
,'Equal' as Pattern_Type
,UserName as Pattern
,User_Bucket
,User_Department
,User_SubDepartment
,User_Region
,1 as Priority
,UserName
,UserHash
from dim_user
;

/*{{save:users_total.csv}}*/
select cast(cast(count(*) as BigInt format'ZZZ,ZZZ,ZZZ,ZZZ') as varchar(32)) as Total_User_Cnt
from dim_user
;

/* End COA: Dim_User */
