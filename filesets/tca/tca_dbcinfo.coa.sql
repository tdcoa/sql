

/*{{save:dbcinfo.csv}}*/
/*{{load:APP_TCA_TMP.stg_tca_dbcinfo}}*/
/*{{call:APP_TCA_TBL.sp_tca_dbcinfo()}}*/
select
  '{siteid}' as Site_ID
, InfoKey
, InfoData
from dbc.dbcinfo as a
