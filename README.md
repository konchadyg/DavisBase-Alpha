# DavisBase-Alpha
Implement a Database engine from scratch.
----------------------------------------------------
DAVISBASE README [CS6360.003]
----------------------------------------------------
Author: KONCHADY GAURAV SHENOY
----------------------------------------------------
STEPS:
1. EXTRACT PROJECT FOLDER AND OPEN IN NETBEANS (V 8.2 OR LATER)
2. CLEAN, BUILD AND RUN

----------------------------------------------------
COMMAND INSTRUCTIONS:
----------------------------------------------------
START DAVISBASE AND TYPE HELP.

SUPPORTED COMMANDS:
All commands below are case insensitive
> SHOW TABLES
	Display the names of all tables.
	
> DESCRIBE TABLE <table_name>
	Print Table Metadata (columns and their datatypes).
	
> SELECT * FROM <table_name> [WHERE <condition>];
	Display all table records and columns whose optional <condition> is <column_name> = <value>.
	
> DROP TABLE <table_name>;
	Remove table data (i.e. all records) and its schema.
	
> INSERT INTO <table_name> values (<COMMA SEPARATED VALUES IN ORDINAL ORDER EXCEPT ROW_ID> )
	FOR INSERT, VALUES ARE SUPPLIED IN ORDINAL POSITION. 
	PRIMARY KEY VALUE SHOULD NOT BE GIVEN. COLUMN_LIST NEED NOT BE PROVIDED.
	
>CREATE TABLE table_name (
 row_id INT PRIMARY KEY,
 column_name2 data_type2 [NOT NULL],
 column_name3 data_type3 [NOT NULL],
 ...
 );
	To create a new table. row_id INT PRIMARY KEY is compulsory, otherwise the table will not function properly.
> VERSION;
	Display the program version.
	
> HELP;
	Display this help information.
	
> EXIT; OR QUIT;
	Exit the program.



