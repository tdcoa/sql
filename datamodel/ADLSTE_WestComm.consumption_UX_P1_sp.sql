REPLACE PROCEDURE adlste_westcomm.
consumption_UX_P1_sp()
BEGIN
 --call sp_audit_Account_Name ('adlste_coa.stg_dat_dbcinfo', 'sp_dat_dbcinfo', '{}');
 --call sp_audit_Site_ID ('adlste_coa.stg_dat_dbcinfo', 'sp_dat_dbcinfo', '{}');

 delete from adlste_westcomm.consumption_UX_P1_v3
 where   (SiteID, LogDate) in
  (Select SiteID, LogDate From adlste_westcomm.consumption_UX_P1_stg);

 Insert into adlste_westcomm.consumption_UX_P1_v3
 Select * From adlste_westcomm.consumption_UX_P1_stg;
END;
