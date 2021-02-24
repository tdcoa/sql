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
  select
   SiteID       as Site_ID
  ,'Equal'      as Pattern_Type
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
  ,coalesce(p.Priority,1e6)(int) as Priority_
  ,coalesce(p.Pattern_Type,'Equal')  as Pattern_Type
  ,coalesce(p.Pattern, o.UserName) as Pattern
  ,coalesce(p.SiteID, 'None')        as SiteID_
  ,o.Active_Flag
  ,{startdate} as StartDate
  ,{enddate} as EndDate
  ,Query_Cnt
  ,Query_Complexity_Score
  ,CPU_Sec
  ,IOGB
  ,Runtime_Sec
  ,Error_Cnt
  from (select
         trim(DatabaseName) as UserName
        ,case when Active_UserName is null then 0 else 1 end (ByteInt) as Active_Flag
        ,Query_Cnt
        ,Query_Complexity_Score
        ,CPU_Sec
        ,IOGB
        ,Runtime_Sec
        ,Error_Cnt
        ,substr(Username,1,3) as first3
        ,substr(Username,floor(character_length(Username)/2)-1,3) as middle3
        ,substr(Username,character_length(Username)-3,3) as last3
        /* generate UserHash value */
        ,trim(cast(from_bytes(hashrow( Username),'base16') as char(9))) ||
         trim(cast(from_bytes(hashrow( first3  ),'base16') as char(9))) ||
         trim(cast(from_bytes(hashrow( middle3 ),'base16') as char(9))) ||
         trim(cast(from_bytes(hashrow( last3   ),'base16') as char(9))) as UserHash
        from dbc.DatabasesV as dv
        left outer join
             (
                 /* determine active users (during timeframe) and their impact */
                 Select
                  Active_UserName
                 ,sum(Query_Cnt) as Query_Cnt
                 ,avg(Query_Complexity_Score) as Query_Complexity_Score
                 ,sum(CPU_Sec) as CPU_Sec
                 ,sum(IOGB) as IOGB
                 ,sum(Runtime_Sec) as Runtime_Sec
                 ,sum(Error_Cnt) as Error_Cnt
                 from (
                        select UserName as Active_UserName
                        ,zeroifnull(sum(cast(Statements as BigInt))) as Query_Cnt
                        ,zeroifnull(avg(cast(NumSteps as BigInt))) as Query_Complexity_Score
                        ,zeroifnull(sum(cast(ParserCPUTime+AMPCPUTime as Decimal(32,2)))) as CPU_Sec
                        ,zeroifnull(cast(sum(ReqIOKB(BigInt))/1e6 as Decimal(32,3))) as IOGB
                        ,zeroifnull(sum(cast(TotalFirstRespTime as Decimal(32,6)))) as Runtime_Sec
                        ,zeroifnull(sum(cast((case when ErrorCode not in(0,3158) then 1 else 0 end) as decimal(32,0)))) as Error_Cnt
                        from dbc.QryLogV
                        where cast(starttime as DATE) between {startdate} and {enddate}
                        group by 1
                            union
                        select UserName as Active_UserName
                        ,zeroifnull(sum(cast(QueryCount as BigInt))) as Query_Cnt
                        ,null as Query_Complexity_Score
                        ,zeroifnull(sum(cast(ParserCPUTime+AMPCPUTime as Decimal(32,2)))) as CPU_Sec
                        ,zeroifnull(cast(sum(ReqPhysIOKB(BigInt))/1e6 as Decimal(32,3))) as IOGB
                        ,0 as Runtime_Sec
                        ,0 as Error_Cnt
                        from dbc.QryLogSummaryV
                        where cast(starttime as DATE) between {startdate} and {enddate}
                        group by 1
                      ) as au1
                 group by Active_UserName
             ) as au2
          on DatabaseName = Active_UserName
        where DBKind = 'U'
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

drop table "dim_tdinternal_databases.csv"
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
,Active_Flag
,Query_Cnt
,Query_Complexity_Score
,CPU_Sec
,IOGB
,Runtime_Sec
,Error_Cnt
from dim_user
;

/*{{save:users_total.csv}}*/
select
 '{siteid}' as Site_ID
,cast(cast(count(*) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as Total_User_Cnt
,cast(cast(count(case when User_Bucket = 'TDInternal' then NULL else username end) as format'ZZZ,ZZZ,ZZ9') as varchar(32)) as Total_LessInternal_Cnt
,min(StartDate(DATE)) as StartDate
,max(EndDate(DATE)) as EndDate
,max(EndDate(DATE)) - min(StartDate(DATE)) (INT) as Day_Count
from dim_user
;

/*{{save:users_active.csv}}*/
select
 '{siteid}' as Site_ID
,cast(cast(count(*) as BigInt format'ZZZ,ZZZ,ZZZ,ZZ9') as varchar(32)) as Active_User_Cnt
,cast(cast(count(case when User_Bucket = 'TDInternal' then NULL else username end) as format'ZZZ,ZZZ,ZZ9') as varchar(32)) as Active_LessInternal_Cnt
,sum(Query_Cnt) as Query_Cnt
,avg(Query_Complexity_Score) as Query_Complexity_Score
,sum(CPU_Sec) as CPU_Sec
,sum(IOGB) as IOGB
,sum(Runtime_Sec) as Runtime_Sec
,sum(Error_Cnt) as Error_Cnt
,min(StartDate(DATE)) as StartDate
,max(EndDate(DATE)) as EndDate
,max(EndDate(DATE)) - min(StartDate(DATE)) (INT) as Day_Count
from dim_user
where Active_Flag = 1
;

/* End COA: Dim_User */
