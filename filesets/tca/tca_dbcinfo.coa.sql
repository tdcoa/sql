

/*{{save:dbcinfo.csv}}*/
/*{{load:{dbprefix}APP_TCA_TMP.stg_tca_dbcinfo}}*/
/*{{call:{dbprefix}APP_TCA_TBL.sp_tca_dbcinfo()}}*/
select
  '{siteid}' as Site_ID
, InfoKey
, InfoData
from dbc.dbcinfo as a
