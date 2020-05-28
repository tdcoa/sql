

/* BUILD VOLATILE "DIM_USER" TABLE */
/*{{temp:dim_user.csv}}*/;
/*{{file:dim_user_override.sql}}*/;

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
        when p.Pattern_Type = 'Equal' and o.UserName = p.Pattern then 1
        when p.Pattern_Type = 'Like'  and o.UserName like p.Pattern then 1
        when p.Pattern_Type = 'RegEx'
         and character_length(regexp_substr(o.UserName, p.Pattern,1,1,'i'))>0 then 1
        else 0 end) = 1
    and SiteID_ in('default','None') or '{siteid}' like SiteID_
  qualify Priority_ = min(Priority_)over(partition by o.UserName)
) with data
primary index (UserName)
on commit preserve rows
;

collect stats on dim_user column(UserName)
;

drop table "dim_user.csv"  /* just in case spool is a concern... */
;

/*{{save:all_users.csv}}*/
Select UserName, UserHash, User_Bucket
,User_Department, User_SubDepartment, User_Region
from dim_user
;


/* long pull from dbql */
Create Volatile Table Top_Users_DBQL  as(
    Select UserName
    ,(YearNumber_of_Calendar(logdate,'ISO')*1000)+
     (MonthNumber_of_Year   (logdate,'ISO')*10)+
     (WeekNumber_of_Month   (logdate,'ISO')) as WeekID
    ,(WeekID/10)(INT) as MonthID
    ,count(distinct LogDate) as Day_Cnt
    ,zeroifnull(sum(cast(Statements as BigInt))) as Query_Cnt
    ,zeroifnull(avg(cast(NumSteps * (character_length(QueryText)/100) as BigInt) )) as Query_Complexity_Score
    ,zeroifnull(sum(cast(ParserCPUTime+AMPCPUTime as decimal(18,2)))) as CPU_Sec
    ,zeroifnull(sum(cast(ReqIOKB/1e6 as decimal(18,0)))) as IOGB
    ,zeroifnull(sum(cast(TotalFirstRespTime as decimal(18,6)))) as Runtime_Sec
    ,zeroifnull(sum(cast((case when dbql.ErrorCode not in(0,3158) then 1 else 0 end) as decimal(9,0)))) as Error_Cnt
    from pdcrinfo.dbqlogtbl_hst as dbql
    where LogDate between date-45 and date-1
    Group by UserName, MonthID, WeekID
) with data
  primary index(UserName)
  on commit preserve rows
;


/* WEEKS must have 7 days (full only) */
delete from Top_Users_DBQL
where WeekID in
 (select WeekID /* nested day_cnt is needed, to max() then sum() */
  from (Select WeekID, MonthID, max(Day_Cnt) as day_cnt
        from Top_Users_DBQL group by 1,2) a
  group by 1 having sum(Day_Cnt)<7)
;


/* Having removed partial weeks, add Month rollup */
insert into Top_Users_DBQL
Select UserName
,null as WeekID
,MonthID
,sum(Day_Cnt)
,sum(Query_Cnt)
,avg(Query_Complexity_Score)
,sum(CPU_Sec)
,sum(IOGB)
,sum(Runtime_Sec)
,sum(Error_Cnt)
from Top_Users_DBQL
Group by UserName, MonthID
;


/* MONTHS must have at least 3 (full) weeks */
delete from Top_Users_DBQL
where WeekID is null
  and MonthID in
(Select MonthID
 from Top_Users_DBQL group by 1
 having count(distinct WeekID)<3)
;


/* Having removed partial Months, add full period rollup
   ...well, kinda.  Full period of based on above minimum
   requirements, i.e.,  full weeks and 3-wk months */
insert into Top_Users_DBQL
Select UserName
,null as WeekID
,null as MonthID
,sum(Day_Cnt)
,sum(Query_Cnt)
,avg(Query_Complexity_Score)
,sum(CPU_Sec)
,sum(IOGB)
,sum(Runtime_Sec)
,sum(Error_Cnt)
from Top_Users_DBQL
where WeekID is null
Group by UserName
;

collect stats on Top_Users_DBQL column(UserName)
;

/*{{save:user_counts.csv}}*/
Select 'Active Users' as tbl, count(distinct UserName) from Top_Users_DBQL
union all
Select 'All Users' as tbl, count(distinct UserName) from  dim_user
;

/*{{save:user_ranks.csv}}*/
Select a.* ,rank()over(partition by MonthID, WeekID order by Combined_Score desc) as Total_Rank
from(
    Select WeekID, MonthID, u.UserName
    ,u.UserHash, u.User_Bucket
    ,u.User_Department, u.User_SubDepartment, u.User_Region
    ,Query_Cnt
    ,rank()over(partition by MonthID, WeekID order by Query_Cnt desc) as Query_Cnt_Rank
    ,Query_Complexity_Score
    ,rank()over(partition by MonthID, WeekID order by Query_Complexity_Score desc) as Query_Complexity_Score_Rank
    ,CPU_Sec
    ,rank()over(partition by MonthID, WeekID order by CPU_Sec desc) as CPU_Sec_Rank
    ,IOGB
    ,rank()over(partition by MonthID, WeekID order by IOGB desc) as IOGB_Rank
    ,Runtime_Sec
    ,rank()over(partition by MonthID, WeekID order by Runtime_Sec desc) as Runtime_Sec_Rank
    ,Error_Cnt
    ,rank()over(partition by MonthID, WeekID order by Error_Cnt desc) as Error_Cnt_Rank
    ,Query_Cnt_Rank + Query_Complexity_Score_Rank + CPU_Sec_Rank + IOGB_Rank + Runtime_Sec_Rank as Combined_Score
    from Top_Users_DBQL as d
    join Dim_User as u
      on d.UserName = u.UserName
) a
;

drop table Top_Users_DBQL;
drop table Dim_User;
