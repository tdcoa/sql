/* illustrates the number of columns required to
   satisfy each number of queries, by bucket

   Parameters:
     - startdate:    {startdate}
     - enddate:      {enddate}
     - siteid:       {siteid}
     - dbqlogtbl:    {dbqlogtbl}

*/


/* qualify list of tables to examine:
   table size:             {table_size_gb}
   columns in table:       {num_col_in_table}
   frequency of table use: {hits_per_day}

   absolute numbers of tables per analysis: {tables_per_analysis}

*/
Create Volatile Table Table_Candidates
( DatabaseName varchar(128)
 ,TableName    varchar(128)
 ,ColumnCount  INTEGER
)
;

Create Volatile Table Table_Candidate_Columns
( DatabaseName varchar(128)
 ,TableName    varchar(128)
 ,ColumnName   varchar(128)
 ,ColumnLength INTEGER
)
;

/*{{file:table_candidates_override.sql}}*/
;


/* object_log, group by db/table/column where obj_Type = 'col'
   count(*)

    per QueryID
*/
Create Volatile Table Query_List
( DatabaseName
 ,TableName
 ,ColumnName
 ,Query)
