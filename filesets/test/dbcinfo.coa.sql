

/*{{save:dbcinfo.csv}}*/
/*{{load:adlste_coa_stg.stg_tca_dbcinfo}}*/
/*{{call:adlste_coa.sp_tca_dbcinfo()}}*/
select
  InfoKey
, InfoData
, '{siteid}' as Site_ID
, cast(CURRENT_TIMESTAMP(0) as timestamp(0) format'yyyy-mm-ddbhh:mi:ss')(char(19)) as TheTimeStamp
from dbc.dbcinfo as a
;
