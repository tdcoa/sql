

/*{{save:dbcinfo.csv}}*/
/*{{load:APP_TCA_TMP.stg_tca_dbcinfo}}*/
/*{{call:APP_TCA_TMP.sp_tca_dbcinfo()}}*/
select
  InfoKey
, InfoData
, '{siteid}' as Site_ID
, cast(CURRENT_TIMESTAMP(0) as timestamp(0) format'yyyy-mm-ddbhh:mi:ss')(char(19)) as TheTimeStamp
from dbc.dbcinfo as a
;
