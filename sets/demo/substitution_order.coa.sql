/* demonstrate substitution order
*/

create volatile table substitution_order
(priority   integer
,comment    varchar(256)
) no primary index
on commit preserve rows
;

insert into substitution_order({priority},'{comment}')
;

/*{{save:sub_order.csv}}*/
select * from  substitution_order
;
