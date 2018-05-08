/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package davisbase;



import static davisbase.FileLocations.*;
import java.util.ArrayList;
import java.io.*;
import static java.lang.System.out;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 * @author konchady
 */
public class Database {
    
    
    public static void showTables() throws IOException
    {
        RandomAccessFile tableraf=null;
        try{
            tableraf = new RandomAccessFile(tablefile, "rw");
            boolean tableexist = false;
            System.out.println("Tables in "+DavisBase.dbname+": ");
            System.out.println("--------------------------------");
            
            while (tableraf.getFilePointer() < tableraf.length()) {
                
                int deleteflag = tableraf.readByte();
                byte namelen = tableraf.readByte();
                byte[] namebytes = new byte[namelen];
                tableraf.read(namebytes, 0, namebytes.length);

                if (deleteflag == 0)
                {
                    tableexist = true;
                    System.out.println(new String(namebytes));
                }
                tableraf.readInt();
            }
                if(!tableexist)
                {
                    System.err.println("DVBERR-007: NO TABLES IN "+DavisBase.dbname+" DATABASE.");
                }
                
                

            
            
        }
        catch(Exception e)
        {
            System.err.println("DVBERR-006: ERROR FETCHING TABLE LIST FROM DATABASE.");
        }
        finally
        {
            if(tableraf!=null)tableraf.close();
        }
                 
        
        
        //throw new UnsupportedOperationException("Not supported yet.");
    }

    static void showDatabase() {
        
        if(DavisBase.dbname!=null)
        {
            System.out.println("CURRENT DATABASE: "+DavisBase.dbname);
        }
        else
        {
            System.out.println("NO ACTIVE DATABASE SELECTED");
        }
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    static void useDatabase(ArrayList<String> queryTokens) throws IOException {
        
        RandomAccessFile dblistfileraf=null;
        try{
            String dbname=queryTokens.get(2);
            dblistfileraf = new RandomAccessFile(FileLocations.databaselistfile, "rw");
            
            boolean db_exists=false;
            while (dblistfileraf.getFilePointer() < dblistfileraf.length()) {
                    int deleteflag = dblistfileraf.readByte();
                    byte dbfilelen = dblistfileraf.readByte();
                    byte[] byteStream = new byte[dbfilelen];
                    dblistfileraf.read(byteStream, 0, byteStream.length);
                    
                    //System.out.println("deleteflag="+deleteflag);
                   // System.out.println("dbfilelen="+dbfilelen);     

                    if ((dbname.equals(new String(byteStream).trim())) && (deleteflag == 0)) {
                        
                        Database.setDatabase(dbname);
                        System.out.println("Using Database...");
                        
                    }
                    else
                    {
                        System.err.println("Database "+dbname+" does not exist.");
                    }
            }
        }
        catch(IndexOutOfBoundsException idxe)
        {
            System.err.println("DVBERR-003: SYNTAX: USE DATABASE <SCHEMA_NAME> ");
        }
        catch(Exception e)
        {
            System.err.println("DVBERR-004: ERROR CONNECTING TO/USING DATABASE. ");
        }
        finally
        {
            if(dblistfileraf!=null)dblistfileraf.close();
        }
        
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }    

    public static void setDatabase(String dbname) {
        
        //DavisBase.dbname=dbname;
        
    }
    
    public static void CreateDatabase(ArrayList<String> queryTokens) {
        

        try
        {
            String dbname=queryTokens.get(2);
            System.out.println("Database name: "+dbname);
            //File dblistfile = new File("DATABASELIST.DAT");
            File fdbschema = new File (dbname + "_TABLES.DAT");
            
            if (!(fdbschema.exists()))
            {
                //Create or reopen main DB list file
                RandomAccessFile dblistfileraf = new RandomAccessFile(FileLocations.databaselistfile, "rw");
                dblistfileraf.seek(dblistfileraf.length());
                dblistfileraf.writeByte(0x0000);
                dblistfileraf.writeByte(dbname.length());
                dblistfileraf.writeBytes(dbname);
                dblistfileraf.close();
                
                //Create the DB table and column files
                RandomAccessFile dbtableraf = new RandomAccessFile(dbname + "_TABLES.DAT", "rw");
                dbtableraf.close();
                
                RandomAccessFile dbcolumnraf = new RandomAccessFile(dbname + "_COLUMNS.DAT", "rw");
                dbcolumnraf.close();                
                
                
                System.out.println("Database "+dbname+" created successfully.");
                
            }
            else
            {
                System.err.println("DVBERR-003: DATABASE "+dbname+" ALREADY EXISTS."); 
            }
            
        }
        catch(IndexOutOfBoundsException idxe)
        {
            System.err.println("DVBERR-002: SYNTAX: CREATE DATABASE <SCHEMA_NAME> ");
        }
        catch(Exception e)
        {
            System.err.println("DVBERR-001: ERROR CREATING DATABASE. ");
            
           //e.printStackTrace();
        }
        
    }

    public static void initializeFiles() {
        
        try {
            File f1 = new File(tablefile);
            File f2 = new File(columnfile);
            
            new File("data/catalog").mkdirs();
            
            RandomAccessFile raf1 = new RandomAccessFile(f1, "rw");
            RandomAccessFile raf2 = new RandomAccessFile(f2, "rw");
            
            raf1.close();
            raf2.close();
            
            
            
            
            //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
        public static void help() {
                out.println("***********************************************************************");
                out.println("SUPPORTED COMMANDS\n");
                out.println("All commands below are case insensitive\n");
                out.println("SHOW TABLES;");
                out.println("\tDisplay the names of all tables.\n");
                //printCmd("SELECT * FROM <table_name>;");
                //printDef("Display all records in the table <table_name>.");
                out.println("SELECT * FROM <table_name> [WHERE <condition>];");
                out.println("\tDisplay all table records and columns whose optional <condition>");
                out.println("\tis <column_name> = <value>.\n");
                out.println("DROP TABLE <table_name>;");
                out.println("\tRemove table data (i.e. all records) and its schema.\n");
                out.println("INSERT INTO <table_name> values (<COMMA SEPARATED VALUES IN ORDINAL ORDER EXCEPT ROW_ID> )");
                out.println("UPDATE TABLE <table_name> SET <column_name> = <value> [WHERE <condition>];");
                out.println("\tModify records data whose optional <condition> is\n");
                out.println("CREATE TABLE table_name (\n" +
                            "row_id INT PRIMARY KEY,\n" +
                            "column_name2 data_type2 [NOT NULL],\n" +
                            "column_name3 data_type3 [NOT NULL],\n" +
                            "...\n" +
                            ");\n"
                        + "\tTo create a new table. row_id INT PRIMARY KEY is compulsory, otherwise the table will not function properly.");
                out.println("VERSION;");
                out.println("\tDisplay the program version.\n");
                out.println("HELP;");
                out.println("\tDisplay this help information.\n");
                out.println("EXIT;");
                out.println("\tExit the program.\n");
                out.println("***********************************************************************");
        }


    
}
