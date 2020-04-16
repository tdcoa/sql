Create table adlste_coa.coat_dim_feature as
(Select * from "dim_feature.csv"
) with no data
primary index (account, bitpos  )
;

replace view adlste_coa.coa_dim_feature as
locking row for access
select * from adlste_coa.coat_dim_feature
;
