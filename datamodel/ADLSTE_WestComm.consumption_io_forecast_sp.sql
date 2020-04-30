REPLACE PROCEDURE adlste_westcomm.
consumption_io_forecast_sp
( fileset_version VARCHAR(128) )
BEGIN
  --call sp_audit_Account_Name ('adlste_coa.stg_dat_dbcinfo', 'sp_dat_dbcinfo', '{}');
  --call sp_audit_Site_ID ('adlste_coa.stg_dat_dbcinfo', 'sp_dat_dbcinfo', '{}');

  delete from adlste_westcomm.consumption_io_forecast_v2
  where   (SiteID, "Log Date") in
   (Select SiteID, "Log Date" From adlste_westcomm.consumption_io_forecast_stg);

  Insert into adlste_westcomm.consumption_io_forecast_v2
  Select * From adlste_westcomm.consumption_io_forecast_stg;
END;
