


Create volatile table siteid_x_account as
(
    select
     Acct_Name  as AccountName
    ,a.Site_ID  as SiteID
    from adlste_coa.coat_mat_sales_role_qlid a
    join (
        Select distinct Site_ID from adlste_coa.coat_dim_cis_siteid
        where System_use  not in('test','development','QA')  /* prod only */
        qualify LogDate = max(logDate) over(partition by Site_ID)) b
    on a.Site_ID = b.Site_ID
    where assoc_qlid in(select QLID from adlste_coa.coat_dim_csm_territory)
    Group by 1,2
) with data no primary index on commit preserve rows;


/*{{save:siteid_x_account.csv}}*/
select  * from siteid_x_account  order by 1,2;


/*{{save:csm_mbo_CPU.csv}}*/
locking row for access
Select
  case when Account_Name is null then '== TOTALS ==' else Account_Name end as Account_Name
 ,case when Site_ID is null then '== SUBTOTALS ==' else Site_ID end as Site_ID
/* cpu slope growth */
  ,sum((yr2020q1_Time - yr2020q1_Time_avg) * (yr2020q1_CPU - yr2020q1_CPU_avg))
      / sum((yr2020q1_Time - yr2020q1_Time_avg) * (yr2020q1_Time - yr2020q1_Time_avg)) as yr2020q1_Slope
  ,sum((yr2019_Time - yr2019_Time_avg) * (yr2019_CPU - yr2019_CPU_avg))
      / sum((yr2019_Time - yr2019_Time_avg) * (yr2019_Time - yr2019_Time_avg)) as yr2019_Slope
  ,cast( (yr2020q1_Slope  / nullifzero(yr2019_Slope))-1 as decimal(9,4)) as  Slope_Growth
/* raw cpu growth, avg() to account different #of days */
  ,avg(yr2020q1_CPU) as  yr2020q1_CPUamt
  ,avg(yr2019_CPU) as yr2019_CPUamt
  ,cast((yr2020q1_CPUamt / nullifzero(yr2019_CPUamt))-1 as decimal(9,4)) as  CPU_Growth
from
(
    Select
     Account_Name, Site_ID, LogDate
    ,yr2020q1_CPU
    ,avg(yr2020q1_cpu) over(partition by Site_ID) as  yr2020q1_CPU_avg
    ,yr2020q1_Time
    ,avg(yr2020q1_time) over(partition by Site_ID) as  yr2020q1_Time_avg
    ,yr2019_CPU
    ,avg(yr2019_cpu) over(partition by Site_ID) as  yr2019_CPU_avg
    ,yr2019_Time
    ,avg(yr2019_time) over(partition by Site_ID) as  yr2019_Time_avg
    from
    (
      select
       AccountName as Account_Name
      ,Site_ID
      ,TheDate as LogDate
      ,Eff_Used_CPU_GHz as CPU
      ,case when TheDate between '2020-01-01' and '2020-03-30' then date_order else  null end as yr2020q1_Time
      ,case when TheDate between '2020-01-01' and '2020-03-30' then CPU        else  null end as yr2020q1_CPU
      ,case when TheDate between '2019-01-01' and '2019-12-31' then date_order else  null end as yr2019_Time
      ,case when TheDate between '2019-01-01' and '2019-12-31' then CPU        else  null end as yr2019_CPU
      ,ROW_NUMBER() OVER (partition by Site_ID order by TheDate Desc ) AS date_order
      from adlste_coa.coat_dim_cis_cpu_gtm_system_usage_hist as a
      join siteid_x_account   on a.Site_ID = SiteID
      where LogDate between '2019-01-01' and '2020-12-31'
    ) as a
) as b
Group by rollup(Account_Name, Site_ID)
order by  1,2,3;
