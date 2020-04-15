/*  upload dim_feature.csv from same directory into new Transcend table
*/

/*{{temp:dim_feature.csv}}*/
/*{{file:create_dim_feature.sql}}*/

delete from adlste_coa.dim_feature
  where (Account_Name, DBS_Version, BitPOS) in
        (select Account_Name, DBS_Version, BitPOS
         from "dim_feature.csv")
;

insert into adlste_coa.dim_feature
Select * from "dim_feature.csv"
;
