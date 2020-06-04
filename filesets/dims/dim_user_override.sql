/*
This script is intended to override the dim_user.csv load with
another, pre-existing user/department mapping table.  Specifically
this uses the systemfe.ca_user_xref, but the SQL can be adjusted
for any table.
*/

/*
Delete From dim_user
*/
;

/*
Insert into dim_user
(Username, User_Bucket, Is_Discrete_Human,
 User_Department, User_SubDepartment, User_Region, SiteID_)
Select
,Username       as UserName
,'unassigned'   as User_Bucket
,Department     as User_Department
,SubDepartment  as User_SubDepartment
,Region         as User_Region
,'{siteid}'     as SiteID_
from systemfe.ca_user_xref
*/
;


/*
delete from "dim_user.csv"
*/
;

/*
insert into "dim_user.csv"
  select
  'default' as SiteID_
  ,'Equal'  as Pattern_Type
  ,UserName as Pattern
  ,Department as User_Bucket
  ,Department as User_Department
  ,SubDepartment as User_SubDepartment
  ,Region as User_Region
  ,0 as Priority_
  from systemfe.ca_user_xref
*/
;
