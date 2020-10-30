/*
  parameters:
    site_id   = {siteid}
*/


/*{{save:dbcinfo.{siteid}.csv}}*/
/*{{load:adlste_coa_stg.stg_tca_dbcinfo}}*/
/*{{call:adlste_coa.sp_tca_dbcinfo()}}*/
Select {siteid} as Site_ID, a.*
from dbc.dbcinfo as a;
