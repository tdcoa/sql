/*
 Version: 1.0
 Date: 28-06-2020
 Description:
 - Updated for VCM 2.0 GA
 - Reports CPU hours and Logical IO as Vantage Unit columns(CPU_VU, IO_VU)
   from DBQL tables by date and per hour granularity.
 - Excludes system errors and users for calculating Vantage Units
 - Phase wise Utility Data for Load Jobs
 - Reports from current date to 365 days back (1 year data)
 - Does not handle NOS system errors
 - CPU_VU column gives the CPU based VU
 - IO_VU column gives the Logical IO based VU
 - LogDate will give the dates
 - It will return the data for last one year
*/

/*{{save:{YYYYMM}_{siteid}_VU.csv}}*/
/*{{load:{db_stg}.stg_dat_consumption_vantage_units}}*/
/*{{call:{db_coa}.sp_dat_consumption_vantage_units('v2')}}*/
select
 '{siteid}' as Site_ID,
 cast(coalesce(vu1.logdate, vu4.logdate) as format 'Y4-MM-DD') (char(10)) as LogDate,
 coalesce(vu1.loghr, vu4.loghr) as LogHr,
 zeroifnull(vu1.vantageunithrs) + zeroifnull(vu4.vantageunitutilityhrs) (Decimal(18,2)) as CPU_VU,
 zeroifnull(vu1.vantageunitTB) + zeroifnull(vu4.vantageunitutilityTB) (Decimal(18,2)) as IO_VU
from
    (select cast(firstresptime as date) as LogDate,
      extract(hour from firstresptime) as LogHr,
      zeroifnull(sum(ampcputime + parsercputime + discputime)) / 3600 (format 'ZZ9.999') as VantageUnitHrs,
      zeroifnull(sum(ReqIOKB)) / (1024*1024*1024) (format 'ZZ9.999') as VantageUnitTB
     from dbc.dbqlogtbl
     where logdate between {startdate_rollingyear} and {enddate_rollingyear}
       and errorcode not in (2631, 2646, 3610, 3702, 3710, 3711, 5405, 7453, 7487, 7583, 7596, 9124, 9990)
       and username not in ('vcmuser','PDCRAccess','LockLogShredder','PDCRTPCD','console','tdap_admin','TDPUSER','tdwm',
                            'PDCRAdmin','SystemFe','PDCRCanary1M','PDCRCanary3M','td_ffe_svc_acct','PDCRCanary4M','PDCRCanary0M',
                            'PDCR users','PDCRCanary2M','TDMaps','SysAdmin','Crashdumps','Sys_Calendar')
     group by 1, 2) vu1
full outer join
    (select LogDate, LogHr,
      sum(Phase0IO) as TotalIOP0,
      sum(Phase1IO) as TotalIOP1,
      sum(Phase2IO) as TotalIOP2,
      sum(Phase3IO) as TotalIOP3,
      sum(Phase4IO) as TotalIOP4,
      sum(Phase0CPU) as TotalCPUP0,
      sum(Phase1CPU) as TotalCPUP1,
      sum(Phase2CPU) as TotalCPUP2,
      sum(Phase3CPU) as TotalCPUP3,
      sum(Phase4CPU) as TotalCPUP4,
      zeroifnull(TotalIOP0 + TotalIOP1  + TotalIOP2 + TotalIOP3 + TotalIOP4) / (1024*1024*1024) (format 'ZZ9.999') as VantageUnitUtilityTB,
      zeroifnull(TotalCPUP0 + TotalCPUP1  + TotalCPUP2 + TotalCPUP3 + TotalCPUP4)/3600 (format 'ZZ9.999') as VantageUnitUtilityHrs
     from
     (select
       cast(jobendtime as date) as LogDate,
       extract(hour from jobendtime) as LogHr,
       0 as Phase0CPU,
       0 as Phase0IO,
       case when utilityname = 'FASTLOAD' or utilityname = 'TPTLOAD' or (utilityname = 'FASTEXP' and fastexportnospool = 'Y')
       then phase1totalcputime
       else 0 end as Phase1CPU,
       case when utilityname = 'FASTLOAD' or utilityname = 'TPTLOAD' or (utilityname = 'FASTEXP' and fastexportnospool = 'Y')
       then phase1iokb
       else 0 end as Phase1IO,
       case when utilityname = 'FASTEXP' or utilityname = 'BAR'
       then phase2totalcputime
       else 0 end as Phase2CPU,
       case when utilityname = 'FASTEXP' or utilityname = 'BAR'
       then phase2iokb
       else 0 end as Phase2IO,
       case when utilityname = 'BAR'
       then phase3totalcputime
       else 0 end as Phase3CPU,
       case when utilityname = 'BAR'
       then phase3iokb
       else 0 end as Phase3IO,
       0 as Phase4CPU,
       0 as Phase4IO
       from dbc.dbqlutilitytbl
       where logdate between {startdate_rollingyear} and {enddate_rollingyear}
      ) dbu
      group by 1, 2
  ) vu4
on
vu1.logdate = vu4.logdate and
vu1.loghr = vu4.loghr
;
