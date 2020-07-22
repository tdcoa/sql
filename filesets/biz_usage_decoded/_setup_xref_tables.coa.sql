/* this process will create the xref tables as volatile tables, to make
   the tables available in the user's namespace, and abstract the various
   places it COULD be saved on a customer's system down to this single
   script.

   It is HIGHLY recommended that after you change this script to copy in
   your particular xref location that you copy this completed file to your
   0_override folder, so it persists even while other content is re-down-
   loaded.

*/
CREATE VOLATILE MULTISET TABLE   ca_table_xref
(
  DataDomain   VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
  SubjectArea  VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
  DatabaseName VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC,
  TableName    VARCHAR(255) CHARACTER SET LATIN NOT CASESPECIFIC
) PRIMARY INDEX ( DatabaseName ,TableName )
ON COMMIT PRESERVE ROWS;

/* uncomment this out if the logic works, or, add your own custom here
   also, add a semi-colon to the end

INSERT INTO ca_table_xref  Select * from systemfe.ca_table_xref
*/



CREATE VOLATILE MULTISET TABLE   ca_user_xref
(
  Username      VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
  Department    VARCHAR(255) CHARACTER SET LATIN   NOT CASESPECIFIC,
  SubDepartment VARCHAR(255) CHARACTER SET LATIN   NOT CASESPECIFIC,
  Region        VARCHAR(255) CHARACTER SET LATIN   NOT CASESPECIFIC
) PRIMARY INDEX ( Username )
ON COMMIT PRESERVE ROWS;

/* uncomment this out if the logic works, or, add your own custom here
   also, add a semi-colon to the end

INSERT INTO ca_user_xref  Select * from systemfe.ca_user_xref 
*/
