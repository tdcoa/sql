



/*{{save:tca_resusagespma.{siteid}.{collection}.{dbsversion}.v1.csv}}*/
/*{{load:adlste_coa_stg."coa_tca_resusageSPMA_{collection}_{dbsversion}"}}*/
/*{{call:adlste_coa.sp_tca_resusageSPMA('{collection}','{dbsversion}')}}*/
select '{siteid}' as Site_ID, a.*
from pdcrinfo.resusagespma as a
where theDate between {startdate} and {enddate};
