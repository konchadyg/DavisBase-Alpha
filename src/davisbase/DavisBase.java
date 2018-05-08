/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package davisbase;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author konchady
 */
public class DavisBase {

    /**
     * @param args the command line arguments
     */
    
    //public static String dbname = new String();
    public static final String dbname = "DAVISBASE";
    
    public static void main(String[] args) {
        startMessage();
        Scanner sc;
        Database.initializeFiles();
        //dbname=null;
        String qs="";
        sc = new Scanner(System.in).useDelimiter(";");
        do{
            System.out.print("DAVISBASE> ");
            qs = sc.next().replace("\n", " ").replace("\r", "").trim().toUpperCase();
            //String qs=queryStatement.toUpperCase();
            //qs=qs.substring(0, qs.length()-1);
            if (!(qs.startsWith("SELECT")||qs.startsWith("UPDATE")||qs.startsWith("DELETE")||qs.startsWith("CREATE")||qs.startsWith("DROP")||qs.startsWith("DESCRIBE")||qs.startsWith("DESC")||qs.startsWith("USE")||qs.startsWith("INSERT")))
            {
                switch(qs)
                {
                    case "":            
                    case ";":           break;
                    case "QUIT":
                    case "EXIT":        //saves all table information in non-volatile files to disk
                                        System.out.println("Exit Successful\n===============");
                                        System.exit(0);
                                        break;

                    case "SHOW DATE":
                    case "SHOWDATE":    System.out.println("Today's Date: "+new Date().toString());    
                                        break;
                                        
                    case "SHOW TABLES": {
                                            try {
                                                Database.showTables();
                                            } catch (IOException ex) {
                                                Logger.getLogger(DavisBase.class.getName()).log(Level.SEVERE, null, ex);
                                            }
                                        }
                                        break;
                                        
                    case "SHOW DATABASE": Database.showDatabase();
                                        break;
                                        
                    case "HELP":        Database.help();
                                        break;
                                        
                    case "VERSION":     System.out.println("V1.00000001 ALPHA");
                                        System.out.println("© kxs168430");
                                        break;

                    default:            System.err.println("Unknown Command :\""+qs+"\"");
                                        break;
                }
            }
            else if(qs.startsWith("SELECT"))
            {
                commandParser(qs,DdlDmlVdl.SELECT);
            }
            else if(qs.startsWith("UPDATE"))
            {
                commandParser(qs,DdlDmlVdl.UPDATE);
            }
            else if(qs.startsWith("DELETE"))
            {
                commandParser(qs,DdlDmlVdl.DELETE);
            }
            else if(qs.startsWith("CREATE"))
            {
                commandParser(qs,DdlDmlVdl.CREATE);
            }
            else if(qs.startsWith("DESCRIBE")||qs.startsWith("DESC"))
            {
                commandParser(qs,DdlDmlVdl.DESCRIBE);
            }
            else if(qs.startsWith("DROP"))
            {
                commandParser(qs,DdlDmlVdl.DROP);
            }
            else if(qs.startsWith("USE"))
            {
                commandParser(qs,DdlDmlVdl.USE);
            }
            else if(qs.startsWith("INSERT"))
            {
                commandParser(qs,DdlDmlVdl.INSERT);
            }            
        }while(true);
        
        
        
    }
    
    public static void startMessage()
    {
        System.out.println("========================================================");
        System.out.println("    ____  ___ _    ___________ ____  ___   _____ ______\n" +
"   / __ \\/   | |  / /  _/ ___// __ )/   | / ___// ____/\n" +
"  / / / / /| | | / // / \\__ \\/ __  / /| | \\__ \\/ __/   \n" +
" / /_/ / ___ | |/ // / ___/ / /_/ / ___ |___/ / /___   \n" +
"/_____/_/  |_|___/___//____/_____/_/  |_/____/_____/");
        //System.out.println("========================================================");
        //System.out.println("Welcome to DAVISBASE");
        //System.out.println("--------------------");
        //System.out.println("Sun Apr 01 18:44:55 CDT 2018");
        System.out.println("========================================================");
        System.out.println("              "+new Date().toString());
        System.out.println("========================================================");

    }

    private static void commandParser(String qs, DdlDmlVdl type) {
        
        ArrayList<String> queryTokens = new ArrayList<String>(Arrays.asList(qs.split(" ")));
        
//        for(int i=0;i<queryTokens.size();i++)
//        {
//            System.out.println(queryTokens.get(i));
//        }
        
        switch(type)
        {
            case CREATE:    {
                                switch (queryTokens.get(1)) 
                                {
                                    case "DATABASE":
                                        //System.out.println("DATABASE CREATED");
                                        System.err.println("CREATION NOT PERMITTED - "
                                                + "DATABASE "+DavisBase.dbname+" ONLY PERMITTED FOR USE.");
                                        //Database.CreateDatabase(queryTokens);
                                        break;
                                    case "TABLE":
                                        try{
                                            Tables.createTable(queryTokens,qs);
                                            //System.out.println("TABLE CREATED");
                                        }
                                        catch(IndexOutOfBoundsException iex)
                                        {
                                            System.err.println("DVBERR-007: CREATE TABLE SYNTAX ERROR:\""+qs+"\"");
                                        } catch (Exception ex) {
                                            Logger.getLogger(DavisBase.class.getName()).log(Level.SEVERE, null, ex);
                                        }
                                        break;
                                    default:
                                        System.err.println("UNKNOWN COMMAND :\""+qs+"\"");
                                        break;
                                }
                            }
                            break;
                            
            case DELETE:    {}
                            break;
            
            case DESC:                
            case DESCRIBE:  {
                                switch(queryTokens.get(1))
                                {
                                    case "TABLE":{
                                                    Tables.describeTable(queryTokens.get(2));
                                                 }
                                                    break;
                                                    
                                    default:     System.err.println("DVBERR-021: INCORRECT DESCRIBE SYNTAX: \""+qs+"\"");
                                }
                            }
                            break;
                            
            case DROP:      {
                                switch(queryTokens.get(1))
                                {
                                    case "TABLE": {
                                        Tables.dropTable(queryTokens.get(2));
                                    }
                                    break;
                                    
                                }
                            }
                            break;
                            
            case INSERT:    {
                                try{
                                    if(queryTokens.get(1).equals("INTO"))//&&queryTokens.get(3).equals("VALUES"))
                                    {
                                        //System.out.println("INSERT");
                                        Tables.insertRowIntoTable(queryTokens, qs);
                                    }
                                }
                                catch(IndexOutOfBoundsException ex)
                                {
                                    System.err.println("DVBERR-011: INCOMPLETE INSERT COMMAND: \""+qs+"\"");
                                }
                                catch(Exception e)
                                {
                                    System.err.println("DVBERR-012: ERROR PARSING INSERT STATEMENT");
                                }
            
                            }
                            break;

            case SELECT:    {
                                if(qs.contains("WHERE"))
                                    Tables.selectTableDataWhere(qs);
                                else 
                                    Tables.selectTableData(qs);
                                    
                            }
                            break;

            case USE:    {
                                try{
                                switch (queryTokens.get(1)) 
                                {
                                    case "DATABASE":
                                        //Database.useDatabase(queryTokens);
                                        //System.out.println("DATABASE "+DavisBase.dbname+" NOW IN USE");
                                        System.err.println("DATABASE "+DavisBase.dbname+" ONLY PERMITTED FOR USE.");
                                        //Database.CreateDatabase(queryTokens);
                                        break;

                                    default:
                                        System.err.println("Unknown Command :\""+qs+"\"");
                                        break;
                                }
                                }
                                catch(IndexOutOfBoundsException iex)
                                {
                                    System.err.println("DVBERR-003: SYNTAX: USE DATABASE <SCHEMA_NAME>");
                                } //catch (IOException ex) {
                                   // Logger.getLogger(DavisBase.class.getName()).log(Level.SEVERE, null, ex);
                                //CREATE}
            
                        }
                            break;
                            
            case UPDATE:    {
                                if(queryTokens.get(2).equals("SET"))
                                    Tables.updateTableData(queryTokens,qs);
            
                            }
                            break;
                            

                            
                            
            default:        {
                                System.out.println("Impossible case");
                                //throw new UnsupportedOperationException("Impossible case.");
                            }
                            break;
        }
        
    }
    
    public enum DdlDmlVdl{
        CREATE,DELETE,DESCRIBE,DESC,DROP,INSERT,SELECT,UPDATE,USE;
    }
    
}

// ArrayList<String> queryTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));