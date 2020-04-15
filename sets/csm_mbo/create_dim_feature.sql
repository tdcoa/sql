Create table adlste_coa.dim_feature as
(Select * from "dim_feature.csv"
) with no data
primary index (account, bitpos  )
;
