/*
  parameters:
    site_id   = {siteid}
    startdate = {startdate}
    enddate   = {enddate}
    tablename = {tablename}
    suffix    = {suffix}

  from the system definition:
    collection = {collection}
    dbsversion = {dbsversion}

*/


/*{{save:tca_resusage.{suffix}.{collection}.{dbsversion}.v1--{siteid}.csv}}*/
/*{{load:adlste_coa_stg."coa_tca_resusage{suffix}_{collection}_{dbsversion}"}}*/
/*{{call:adlste_coa.sp_tca_resusage('{suffix}', '{collection}', '{dbsversion}')}}*/
select '{siteid}' as Site_ID, a.*
from {tablename} as a
where theDate between {startdate} and {enddate};
