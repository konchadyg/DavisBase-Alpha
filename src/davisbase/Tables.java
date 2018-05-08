/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package davisbase;

import static davisbase.FileLocations.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author konchady
 */
public class Tables {
    
    public static void createTable(ArrayList queryTokens, String qs) throws IndexOutOfBoundsException, Exception
    {
        String dbname = DavisBase.dbname;
        
        String tablename = queryTokens.get(2).toString();
        
        if(!doesTableExist(tablename))
        {
            try {
                //tokens[0] = "rowid int";
                
                qs = qs.replace('(', '#').replace(',', '#').replace(')', ' ').trim();
                String elements[]=qs.split("#");
                int arraysize=0;
                for (String s:elements)
                {
                    arraysize++;
                }
                if(arraysize>2) {
                    //throw new ArrayIndexOutOfBoundsException();
                
                elements[0]="ROW_ID INT PRIMARY KEY";
                int pkc=0;
                RandomAccessFile tableraf = new RandomAccessFile(tablefile, "rw");
                new File("data/user_data").mkdirs();
                //Make entry into Davisbase table file
                tableraf.seek(tableraf.length());
                tableraf.writeByte(0);
                tableraf.writeByte(tablename.length());
                tableraf.writeBytes(tablename);
                tableraf.writeInt(0);
                tableraf.close();
                
                //Make Entries into the Davisbase column file
                
                RandomAccessFile columnraf = new RandomAccessFile(columnfile, "rw");
                columnraf.seek(columnraf.length());

                
                for (String element : elements) {
                    
                    element=element.trim();
                    if(element.contains("ROW_ID"))
                    {
                        pkc++;
                        if(pkc>=2)continue;
                    }
                    if ((element != null) && (!element.isEmpty())) {
                        columnraf.writeByte(0);
                        if (element.contains("PRIMARY KEY")) {
                            element = element.replace("PRIMARY KEY", "PRIMARYKEY");
                        }
                        if (element.contains("NOT NULLABLE")) {
                            element = element.replace("NOT NULLABLE", "NOTNULLABLE");
                        }
                        
                        String coldef = tablename + "#"+ element.replaceAll("  ", " ").replaceAll(" ", "#").trim();
                        columnraf.writeByte(coldef.length());
                        columnraf.writeBytes(coldef);
                        
                    }
                    
                }
                
                
                columnraf.close();
                
                RandomAccessFile tabledata = new RandomAccessFile(pretable+tablename + ".tbl", "rw");
                tabledata.close();
                System.out.println("TABLE "+tablename+" CREATED SUCCESSFULLY");
            }
                else
                {
                    System.err.println("DVBERR-015: CREATE STATEMENT INCOMPLETE COLUMN DEFINITION");
                }
            } 
            catch (FileNotFoundException ex) {
                Logger.getLogger(Tables.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(Tables.class.getName()).log(Level.SEVERE, null, ex);
            } catch (ArrayIndexOutOfBoundsException ex) {
                
                System.err.println("DVBERR-015: CREATE STATEMENT INCOMPLETE COLUMN DEFINITION>");
                //Logger.getLogger(Tables.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        else
        {
            System.err.println("TABLE "+tablename+" EXISTS IN "+DavisBase.dbname+" DATABASE");
        }
        
        
    }
    
    public static boolean doesTableExist(String tablename)
    {
        //boolean exist=false;
        boolean tablentryexists=false;
        try 
        {
            File tablefile= new File(pretable+tablename+".tbl");
//            RandomAccessFile tableraf = new RandomAccessFile(tablefile,"rw");
//            while (tableraf.getFilePointer() < tableraf.length())
//            {
//                int isDeleted = tableraf.readByte();
//                byte len = tableraf.readByte();
//                byte[] bytes = new byte[len];
//                tableraf.read(bytes, 0, bytes.length);
//                if(tablename.equals(new String(bytes))&&isDeleted==0)
//                {
//                    tablentryexists=true;
//                }
//                
//            }
            
            
            if(tablefile.exists())//||tablentryexists)
            {
                return true;
            }
            
        } 
        catch (Exception e) {
            
            System.err.println("DVBERR-005: TABLE ACCESS ENCOUNTERED AN ERROR");
            return false;
            
        }
        
        return false;
    }
    
    public static void describeTable(String tablename)
    {
        if(Tables.doesTableExist(tablename))
        {
            List<ColumnDS> col = getTableColumns(tablename);
            
            System.out.println("*******************************************************");
            System.out.println(tablename);
            System.out.println("*******************************************************");
            System.out.println("NAME\tDATATYPE\tISPRIMARY\tISNOTNULLABLE");
            for (ColumnDS c:col)
            {
                System.out.println(c.getColName()+"\t"+c.getDataType()+"\t\t"+c.isPrimaryKey()+"\t\t"+c.isNotNull());
            }
            
            System.out.println("*******************************************************");
        }
        else 
        {
            System.err.println("DVBERR-016: NO SUCH TABLE EXISTS");
        }
    }

    public static void dropTable(String tablename) {
        
        try {
            RandomAccessFile tabfile = new RandomAccessFile(tablefile,"rw");
            int flag=1;
            while (tabfile.getFilePointer() < tabfile.length()) {
                int isDeleted = tabfile.readByte();
                byte len = tabfile.readByte();
                byte[] bytes = new byte[len];
                tabfile.read(bytes, 0, bytes.length);
                
                if ((tablename.equals(new String(bytes))) && (isDeleted == 0)) {
                    tabfile.seek(tabfile.getFilePointer() - bytes.length - 2L);
                    tabfile.writeByte(1);
                    flag = isDeleted;
                    break;
                }
                tabfile.readInt();
                
                
            }
            tabfile.close();
            
            deleteColumnByFlagging(tablename);
            
            File file = new File(pretable+tablename+".tbl");
            file.delete();
            
            if (flag == 0)System.out.println("TABLE DROPPED SUCCESSFULLY,");
            else System.err.println("DVBERR-016: TABLE TO DROP DOES NOT EXIST");
            
            
            
            
            //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Tables.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Tables.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void deleteColumnByFlagging(String tablename) {
        try
        {
            if(doesTableExist(tablename))
            {
                RandomAccessFile colfile = new RandomAccessFile(columnfile,"rw");
                while (colfile.getFilePointer() < colfile.length()) {
                    int isDeleted = colfile.readByte();
                    byte len = colfile.readByte();
                    byte[] bytesChunk = new byte[len];
                    colfile.read(bytesChunk, 0, bytesChunk.length);
                    String[] cols = new String(bytesChunk).replaceAll("#", " ").split(" ");
                    if ((cols[0].equals(tablename)) && (isDeleted == 0)) {
                        long ptr = colfile.getFilePointer();
                        colfile.seek(ptr - len - 2L);
                        colfile.writeByte(1);
                        colfile.seek(ptr);
                    }
                    
                    
                }
                colfile.close();
            }
        }
        catch(Exception e)
        {
            System.err.println("DVBERR-008: ERROR DROPPING TABLE COLUMNS");
        }
    }
    
    public static void insertRowIntoTable(ArrayList queryTokens, String query)
    {
        try {
            query = query.replace('(', '#').replace(')', ' ').trim();
            String[] procstmt = query.split("#");
            // INSERT INTO TABLENAME VALUES ...
            String tablename = (String) queryTokens.get(2);
            
            List<ColumnDS> cols = getTableColumns(tablename);
            
            int rowcount = 0;
            
            RandomAccessFile dbtableraf = new RandomAccessFile(tablefile, "rw");
            
            long ptr = -1L; //Set to before offset
            
            while (dbtableraf.getFilePointer() < dbtableraf.length()) {
                dbtableraf.readByte();
                byte len = dbtableraf.readByte();
                byte[] bytes = new byte[len];
                dbtableraf.read(bytes, 0, bytes.length);
                String dbtabname = new String(bytes);
                
                ptr = dbtableraf.getFilePointer();
                rowcount = dbtableraf.readInt();
                
                if (dbtabname.equals(tablename)) {
                    rowcount=rowcount+1;
                    break;
                }
   
            }
            
            procstmt[1] = (rowcount + "," + procstmt[1]);
            String[] valuetokens = procstmt[1].trim().split(",");
            
            int recsize = 0;
            boolean error = false;
            
            if (cols.size() == valuetokens.length) {
                
                for (int i = 0; i < valuetokens.length; i++) {

                    if ((((ColumnDS) cols.get(i)).isNotNull()) || (((ColumnDS) cols.get(i)).isPrimaryKey())) {
                            if ((valuetokens[i] == null) || (valuetokens[i].equals("NULL"))) {
                                    error = true;
                            }
                            if (((ColumnDS) cols.get(i)).isPrimaryKey()) {
                                    error = isKeyExisting("SELECT * FROM " + tablename + " WHERE "
                                                    + ((ColumnDS) cols.get(i)).getColName() + "=" + valuetokens[i]);
                            }
                            if (error)
                                    break;
                    }
                    if (((ColumnDS) cols.get(i)).getDataType().equals("INT")) {
                            recsize += 4;
                    } else if (((ColumnDS) cols.get(i)).getDataType().equals("TINYINT")) {
                            recsize++;     
                    } else if (((ColumnDS) cols.get(i)).getDataType().equals("SMALLINT")) {
                            recsize += 2;
                    } else if (((ColumnDS) cols.get(i)).getDataType().equals("BIGINT")) {
                            recsize += 8;
                    } else if (((ColumnDS) cols.get(i)).getDataType().equals("REAL")) {
                            recsize += 4;
                    } else if (((ColumnDS) cols.get(i)).getDataType().equals("DOUBLE")) {
                            recsize += 8;
                    } else if (((ColumnDS) cols.get(i)).getDataType().equals("DATE")) {
                            recsize += 8;
                    } else if (((ColumnDS) cols.get(i)).getDataType().equals("DATETIME")) {
                            recsize += 8;
                    } else {
                            recsize += valuetokens[i].length() + 1;
                    }                    
                    
                    
                }
                
            }
            
            if(!error)
            {
                dbtableraf.seek(ptr);
                dbtableraf.writeInt(rowcount);
                BPlusTree bptree = new BPlusTree();
                BPlusTree.tableName = pretable+tablename + ".tbl";
                long pointer = bptree.insert(recsize);
                
                RandomAccessFile tableraf = new RandomAccessFile(BPlusTree.tableName, "rw");
                tableraf.seek(pointer);

				for (int i = 0; i < valuetokens.length; i++) {
					if (((ColumnDS) cols.get(i)).getDataType().equals("INT")) {
						tableraf.writeInt(Integer.parseInt(valuetokens[i]));
					} else if (((ColumnDS) cols.get(i)).getDataType().equals("TINYINT")) {
						tableraf.writeByte(Byte.parseByte(valuetokens[i]));
					} else if (((ColumnDS) cols.get(i)).getDataType().equals("SMALLINT")) {
						tableraf.writeInt(Short.parseShort(valuetokens[i]));
					} else if (((ColumnDS) cols.get(i)).getDataType().equals("BIGINT")) {
						tableraf.writeLong(Long.parseLong(valuetokens[i]));
					} else if (((ColumnDS) cols.get(i)).getDataType().equals("REAL")) {
						tableraf.writeFloat(Float.parseFloat(valuetokens[i]));
					} else if (((ColumnDS) cols.get(i)).getDataType().equals("DOUBLE")) {
						tableraf.writeDouble(Double.parseDouble(valuetokens[i]));
					} else if (((ColumnDS) cols.get(i)).getDataType().equals("DATE")) {
						tableraf.writeLong(convertStringToDate(valuetokens[i]));
					} else if (((ColumnDS) cols.get(i)).getDataType().equals("DATETIME")) {
						tableraf.writeLong(Long.parseLong(valuetokens[i]));
					} else {
						tableraf.writeByte(valuetokens[i].length());
						tableraf.writeBytes(valuetokens[i]);
					}
				}
                                
                                tableraf.close();
                                System.out.println("ROW INSERTED");
                                
            }
            else
            {
				System.out.println("DVBERR-10: PRIMARY KEY SHOULD BE UNIQUE");
				System.out.println("OR");
				System.out.println("NULL FIELD CANNOT BE NULL");                
            }
            dbtableraf.close();
    
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Tables.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Tables.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        
        
        
    }
    
    
    public static List<ColumnDS> getTableColumns(String tablename)
    {
		List<ColumnDS> columns = new ArrayList<ColumnDS>();
		try {
			if (doesTableExist(tablename)) {
				RandomAccessFile colraf = new RandomAccessFile(columnfile, "rw");
				while (colraf.getFilePointer() < colraf.length()) {
					int isDeleted = colraf.readByte();
					byte length = colraf.readByte();
					byte[] bytes = new byte[length];
					colraf.read(bytes, 0, bytes.length);
					String[] column = new String(bytes).replaceAll("#", " ").split(" ");
					if ((column[0].equals(tablename)) && (isDeleted == 0)) {
						ColumnDS c = new ColumnDS();
						c.setColName(column[1]);
						c.setDataType(column[2]);
						c.setPrimaryKey(false);
						c.setNotNull(false);
						if (column.length == 4) {
							if (column[3].equals("PRIMARYKEY")) {
								c.setPrimaryKey(true);
							} else if (column[3].equals("NOTNULLABLE")) {
								c.setNotNull(true);
							}
						}
						columns.add(c);
					}
				}
				colraf.close();
			}
		} catch (Exception e) {
			System.out.println("DVBERR-018: ERROR IN COLUMNS PARSING");
		}

		return columns;
	}
    
    public static void selectTableData(String qs)
    {
		try {
			String[] tokens = qs.split(" ");
			String tablename = tokens[3].trim();
			if (doesTableExist(tablename)) {
				RandomAccessFile table = new RandomAccessFile(pretable+tablename+".tbl","rw");
                                List<ColumnDS> col = getTableColumns(tablename);
                                System.out.println("*******************************************************");
                                for (ColumnDS c:col)
                                {
                                    System.out.print(c.getColName()+"\t");
                                }
                                System.out.println();
                                System.out.println("*******************************************************");

                                
                                
				if (table.length() > 0L) {
					List<ColumnDS> columns = getTableColumns(tablename);
					table.readByte();
					int cells = table.readByte();
					table.readShort();
					long rightPointer = table.readInt();
					ArrayList<Short> cellPointers = new ArrayList<Short>();
					for (int i = 0; i < cells; i++) {
						cellPointers.add(Short.valueOf(table.readShort()));
					}
					boolean nextPage = true;
					while (nextPage) {
						for (int i = 0; i < cellPointers.size(); i++) {
							table.seek(((Short) cellPointers.get(i)).shortValue());
							for (ColumnDS column : columns) {
								if (column.getDataType().equals("INT")) {
									System.out.print(" " + table.readInt());
								} else if (column.getDataType().equals("TINYINT")) {
									System.out.print(" " + table.readByte());
								} else if (column.getDataType().equals("SMALLINT")) {
									System.out.print(" " + table.readShort());
								} else if (column.getDataType().equals("BIGINT")) {
									System.out.print(" " + table.readLong());
								} else if (column.getDataType().equals("REAL")) {
									System.out.print(" " + table.readFloat());
								} else if (column.getDataType().equals("DOUBLE")) {
									System.out.print(" " + table.readDouble());
								} else if (column.getDataType().equals("DATE")) {
									System.out.print(" " + convertDateToStringObject(table.readLong()));
								} else if (column.getDataType().equals("DATETIME")) {
									System.out.print(" " + convertDateTimeToStringObject(table.readLong()));
								} else {
									int length = table.readByte();
									byte[] bytes = new byte[length];
									table.read(bytes, 0, bytes.length);
									System.out.print(" " + new String(bytes));
								}
							}
							System.out.println();
						}
						if (rightPointer != 0L) {
							table.seek(rightPointer);
							table.readByte();
							cells = table.readByte();
							table.readShort();
							rightPointer = table.readInt();
							cellPointers = new ArrayList<Short>();
							for (int i = 0; i < cells; i++) {
								cellPointers.add(Short.valueOf(table.readShort()));
							}
						} else {
							nextPage = false;
						}
					}
                                        System.out.println("*******************************************************");
				} else {
					System.out.println("NO ROWS");
				}
				table.close();
			}
                        else
                        {
                            System.err.println("DVBERR-016: NO SUCH TABLE EXISTS");
                        }
		} catch (Exception e) {
			System.out.println("DVBERR-017: ERROR DURING INSERT OPERATION");
		}
	}
    
    public static void selectTableDataWhere(String qs) {
		try {
			String[] tokens = qs.split(" ");
			String tableName = tokens[3].trim();
			if (doesTableExist(tableName)) {
				String f = qs.substring(qs.indexOf("WHERE") + 5, qs.length()).trim();
				String[] fa = f.trim().split("=");
                                //fa[1]=fa[1].replace('\'', ' ').trim();
				RandomAccessFile tableraf = new RandomAccessFile(pretable + tableName + ".tbl","rw");
				if (tableraf.length() > 0L) {
					java.util.List<ColumnDS> columns = getTableColumns(tableName);

					tableraf.readByte();
					int cells = tableraf.readByte();
					tableraf.readShort();
					long rightPointer = tableraf.readInt();
					ArrayList<Short> cellPointers = new ArrayList<Short>();
					for (int i = 0; i < cells; i++) {
						cellPointers.add(Short.valueOf(tableraf.readShort()));
					}
					boolean nextPage = true;
					while (nextPage) {
						for (int i = 0; i < cellPointers.size(); i++) {
							tableraf.seek(((Short) cellPointers.get(i)).shortValue());
							String output = "";
							Boolean isDisplay = false;
							for (ColumnDS column : columns) {
								if (column.getDataType().equals("INT")) {
									String value = "" + tableraf.readInt();
									if (column.getColName().equals(fa[0])) {
										if (!value.equals(fa[1].trim()))
											break;
										isDisplay = true;
									}

									output = output + " " + value;
								} else if (column.getDataType().equals("TINYINT")) {
									String value = "" + tableraf.readByte();
									if (column.getColName().equals(fa[0])) {
										if (!value.equals(fa[1].trim()))
											break;
										isDisplay = true;
									}

									output = output + " " + value;
								} else if (column.getDataType().equals("SMALLINT")) {
									String value = "" + tableraf.readShort();
									if (column.getColName().equals(fa[0])) {
										if (!value.equals(fa[1].trim()))
											break;
										isDisplay = true;
									}

									output = output + " " + value;
								} else if (column.getDataType().equals("BIGINT")) {
									String value = "" + tableraf.readLong();
									if (column.getColName().equals(fa[0])) {
										if (!value.equals(fa[1].trim()))
											break;
										isDisplay = true;
									}

									output = output + " " + value;
								} else if (column.getDataType().equals("REAL")) {
									String value = "" + tableraf.readFloat();
									if (column.getColName().equals(fa[0])) {
										if (!value.equals(fa[1].trim()))
											break;
										isDisplay = true;
									}

									output = output + " " + value;
								} else if (column.getDataType().equals("DOUBLE")) {
									String value = "" + tableraf.readDouble();
									if (column.getColName().equals(fa[0])) {
										if (!value.equals(fa[1].trim()))
											break;
										isDisplay = true;
									}

									output = output + " " + value;
								} else if (column.getDataType().equals("DATE")) {
									String value = convertDateToStringObject(tableraf.readLong());
									if (column.getColName().equals(fa[0])) {
										if (!value.equals(fa[1].trim()))
											break;
										isDisplay = true;
									}

									output = output + " " + value;
								} else if (column.getDataType().equals("DATETIME")) {
									String value = convertDateTimeToStringObject(tableraf.readLong());
									if (column.getColName().equals(fa[0])) {
										if (!value.equals(fa[1].trim()))
											break;
										isDisplay = true;
									}

									output = output + " " + value;
								} else {
									int length = tableraf.readByte();
									byte[] bytes = new byte[length];
									tableraf.read(bytes, 0, bytes.length);
									String value = "" + new String(bytes);
									if (column.getColName().equals(fa[0])) {
										if (value.equals(fa[1].trim()))
											break;
										isDisplay = true;
									}

									output = output + " " + value;
								}
								if (isDisplay)
									System.out.println(output);
							}
						}
						if (rightPointer != 0L) {
							tableraf.seek(rightPointer);
							tableraf.readByte();
							cells = tableraf.readByte();
							tableraf.readShort();
							rightPointer = tableraf.readInt();
							cellPointers = new ArrayList<Short>();
							for (int i = 0; i < cells; i++) {
								cellPointers.add(Short.valueOf(tableraf.readShort()));
							}
						} else {
							nextPage = false;
						}
					}
				} else {
					System.out.println("No record present");
				}
				tableraf.close();
			}
		} catch (Exception e) {
			System.out.println("Error, While fectching records from table");
		}
	}  
    
    public static void updateTableData(ArrayList queryTokens,String qs)
    {
        try{
            String tablename = (String)queryTokens.get(1);
            String updateString = (String)queryTokens.get(3);
            String[] updateTokens = updateString.split("=");
            String newvalue=updateTokens[1].replaceAll("\"", " ").replace(" ","");
            //String str1 = qs.substring(0, qs.indexOf("WHERE"));
            //String str2 = qs.substring(qs.indexOf("WHERE")+6, qs.length());
            
            if (doesTableExist(tablename)) {
                RandomAccessFile table = new RandomAccessFile(pretable+tablename+".tbl","rw");
                if (table.length() > 0L) {
                    List<ColumnDS> cols = getTableColumns(tablename);
                    table.readByte();
                    int cells = table.readByte();
                    table.readShort();
                    long rightPointer = table.readInt();
                    ArrayList<Short> cellPointers = new ArrayList<Short>();
                    for (int i = 0; i < cells; i++) {
			cellPointers.add(Short.valueOf(table.readShort()));
                    }
                    boolean nextPage = true;
                    
                    while (nextPage) {
                        for (int i = 0; i < cellPointers.size(); i++){
                            table.seek((cellPointers.get(i)));
			    //String output = "";
                            for (ColumnDS column : cols) {
                                if(column.getColName().equals(updateTokens[0]))
                                {
                                    table.seek(table.getFilePointer()+new Byte(""+4));
                                    if (column.getDataType().equals("INT")) {
                                                int x= table.readInt();
                                                try{
                                                    int y=Integer.parseInt(newvalue);
                                                     table.seek(table.getFilePointer()-new Byte(""+4));
                                                     table.writeInt(y);
                                                   }
                                                catch(NumberFormatException nx){
                                                    System.err.println("DVBERR-022: BAD VALUE");
                                                    break;
                                                }
//                                                if (column.getColName().equals(fa[0])) {
//                                                        if (!value.equals(fa[1].trim()))
//                                                                break;
//                                                        //isDisplay = true;
//                                                }

                                                //output = output + " " + value;
                                        } else if (column.getDataType().equals("TINYINT")) {
                                                byte x = table.readByte();
                                                
                                                try{
                                                    //int y=Integer.parseInt(newvalue);
                                                     table.seek(table.getFilePointer()-new Byte(""+1));
                                                     table.writeByte(Byte.valueOf(newvalue));
                                                   }
                                                catch(NumberFormatException nx){
                                                    System.err.println("DVBERR-022: BAD VALUE");
                                                    break;
                                                }

                                                //output = output + " " + value;
                                        } else if (column.getDataType().equals("SMALLINT")) {
                                                short x = table.readShort();
                                                try{
                                                    //int y=Integer.parseInt(newvalue);
                                                     table.seek(table.getFilePointer()-new Byte(""+2));
                                                     table.writeShort(Short.valueOf(newvalue));
                                                   }
                                                catch(NumberFormatException nx){
                                                    System.err.println("DVBERR-022: BAD VALUE");
                                                    break;
                                                }
                                        } else if (column.getDataType().equals("BIGINT")) {
                                                String x = "" + table.readLong();
                                                try{
                                                    //int y=Integer.parseInt(newvalue);
                                                     table.seek(table.getFilePointer()-new Byte(""+8));
                                                     table.writeLong(Long.getLong(newvalue));
                                                   }
                                                catch(NumberFormatException nx){
                                                    System.err.println("DVBERR-022: BAD VALUE");
                                                    break;
                                                }

                                                //output = output + " " + value;
                                        } else if (column.getDataType().equals("REAL")) {
                                                String x = "" + table.readFloat();
                                                try{
                                                    //int y=Integer.parseInt(newvalue);
                                                     table.seek(table.getFilePointer()-new Byte(""+4));
                                                     table.writeFloat(Float.parseFloat(newvalue));
                                                   }
                                                catch(NumberFormatException nx){
                                                    System.err.println("DVBERR-022: BAD VALUE");
                                                    break;
                                                }
                                        } else if (column.getDataType().equals("DOUBLE")) {
                                                String x = "" + table.readDouble();
                                                try{
                                                    //int y=Integer.parseInt(newvalue);
                                                     table.seek(table.getFilePointer()-new Byte(""+8));
                                                     table.writeDouble(Double.parseDouble(newvalue));
                                                   }
                                                catch(NumberFormatException nx){
                                                    System.err.println("DVBERR-022: BAD VALUE");
                                                    break;
                                                }
                                        } else if (column.getDataType().equals("DATE")) {
                                                String x = convertDateToStringObject(table.readLong());
                                                try{
                                                    //int y=Integer.parseInt(newvalue);
                                                     table.seek(table.getFilePointer()-new Byte(""+8));
                                                     table.writeLong(Long.parseLong(newvalue));
                                                   }
                                                catch(NumberFormatException nx){
                                                    System.err.println("DVBERR-022: BAD VALUE");
                                                    break;
                                                }


                                                //output = output + " " + value;
                                        } else if (column.getDataType().equals("DATETIME")) {
                                                String x = convertDateTimeToStringObject(table.readLong());
                                                try{
                                                    //int y=Integer.parseInt(newvalue);
                                                     table.seek(table.getFilePointer()-new Byte(""+8));
                                                     table.writeLong(Long.parseLong(newvalue));
                                                   }
                                                catch(NumberFormatException nx){
                                                    System.err.println("DVBERR-022: BAD VALUE");
                                                    break;
                                                }
                                        } else {
                                                int length = table.readByte();
                                                byte[] bytes = new byte[length];
                                                table.read(bytes, 0, bytes.length);
                                                table.seek(table.getFilePointer()-bytes.length);
                                                table.write(newvalue.getBytes());
                                                if(newvalue.getBytes().length>length)
                                                {
                                                    table.write(" ".getBytes(),0, newvalue.getBytes().length-length);
                                                }

                                        }
								//if (isDisplay)
									//System.out.println(output);
                                }
                                
                            }
                        }
                    }
                }
            }
            
        }
        catch(Exception e)
        {
            System.err.println("DVBERR-020: ERROR PROCESSING UPDATE");
        }
    }

    private static boolean isKeyExisting(String command) {
        

        try {
            
        String[] sttokens = command.split(" ");
        String tablename = sttokens[3].trim();
        
            if(doesTableExist(tablename))
            {

                String f = command.substring(command.indexOf("WHERE") + 5, command.length()).trim();
                String[] fa = f.split("=");

                RandomAccessFile tableraf = new RandomAccessFile(pretable+tablename + ".tbl","rw");
                
                if (tableraf.length() > 0L) {
                    List<ColumnDS> cols = getTableColumns(tablename);
                    
                    tableraf.readByte();
                    int cells = tableraf.readByte();
                    tableraf.readShort();
                    
                    long rightptr = tableraf.readInt();
                    ArrayList<Short> cellptr = new ArrayList();
                    
                    for (int i = 0; i < cells; i++)
                    {
                        cellptr.add(Short.valueOf(tableraf.readShort()));
                    }
                    
                    boolean nextpage = true;
                    while (nextpage) {
                        for (int i = 0; i < cellptr.size(); i++) {
                            tableraf.seek(((Short) cellptr.get(i)).shortValue());
                            
                            for (ColumnDS column : cols) {
                                
                                if (column.getDataType().equals("INT")) {
                                String value = "" + tableraf.readInt();
                                    if (column.getColName().equals(fa[0])) {
                                            if (!value.equals(fa[1]))
                                                    break;
                                            return true;
                                    }

                                }
                                else if (column.getDataType().equals("TINYINT")){
                                    String value = "" + tableraf.readByte();
                                    if (column.getColName().equals(fa[0])) {
                                            if (!value.equals(fa[1]))
                                                    break;
                                            return true;
                                    }
                                    
                                }
                                else if (column.getDataType().equals("SMALLINT")){
                                    String value = "" + tableraf.readShort();
                                    if (column.getColName().equals(fa[0])) {
                                            if (!value.equals(fa[1]))
                                                    break;
                                            return true;
                                    }
                                    
                                }
                                else if (column.getDataType().equals("BIGINT")){
                                    String value = "" + tableraf.readLong();
                                    if (column.getColName().equals(fa[0])) {
                                            if (!value.equals(fa[1]))
                                                    break;
                                            return true;
                                    }
                                    
                                }                                
                                else if (column.getDataType().equals("REAL")){
                                    String value = "" + tableraf.readFloat();
                                    if (column.getColName().equals(fa[0])) {
                                            if (!value.equals(fa[1]))
                                                    break;
                                            return true;
                                    }
                                    
                                }
                                else if (column.getDataType().equals("DOUBLE")){
                                    String value = "" + tableraf.readFloat();
                                    if (column.getColName().equals(fa[0])) {
                                            if (!value.equals(fa[1]))
                                                    break;
                                            return true;
                                    }
                                    
                                }
                                else if (column.getDataType().equals("DATE")){
                                    String value = convertDateToStringObject(tableraf.readLong());
                                    if (column.getColName().equals(fa[0])) {
                                            if (!value.equals(fa[1]))
                                                    break;
                                            return true;
                                    }
                                    
                                }
                                else if (column.getDataType().equals("DATETIME")){
                                    String value = convertDateTimeToStringObject(tableraf.readLong());
                                    if (column.getColName().equals(fa[0])) {
                                            if (!value.equals(fa[1]))
                                                    break;
                                            return true;
                                    }
                                    
                                }
                                else
                                {
                                    int length = tableraf.readByte();
                                    byte[] bytes = new byte[length];
                                    tableraf.read(bytes, 0, bytes.length);
                                    String value = " " + new String(bytes);
                                    if (column.getColName().equals(fa[0])) {
                                            if (!value.equals(fa[1]))
                                                    break;
                                            return true;
                                    }                                 
                                }
                            }
                        }
                        
                        if (rightptr != 0L) {
                            tableraf.seek(rightptr);
                            tableraf.readByte();
                            cells = tableraf.readByte();
                            tableraf.readShort();
                            rightptr = tableraf.readInt();
                            cellptr = new ArrayList<Short>();
                            for (int i = 0; i < cells; i++) {
                                    cellptr.add(Short.valueOf(tableraf.readShort()));
                            }
                    } else {
                            nextpage = false;
                    }                            
                        }
                    }
                tableraf.close();
                    
                }
                
                
            } catch (IOException ex) {
            Logger.getLogger(Tables.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return false;
   
    }

    private static String convertDateToStringObject(long date) {
        String datepattern = "MM:dd:yyyy";
        SimpleDateFormat sdformat = new SimpleDateFormat(datepattern);
        Date newdate = new Date(date);
        return sdformat.format(newdate);
    }

    private static String convertDateTimeToStringObject(long datetime) {
        String dtpattern = "YYYY-MM-DD_hh:mm:ss";
        SimpleDateFormat dtformat = new SimpleDateFormat(dtpattern);
        Date newdate = new Date(datetime);
        return dtformat.format(newdate);
    }
    
    private static long convertStringToDate(String dateString) {
            String pattern = "MM:dd:yyyy";
            SimpleDateFormat format = new SimpleDateFormat(pattern);
            try {
                    Date date = format.parse(dateString);
                    return date.getTime();
            } catch (ParseException e) {
                    e.printStackTrace();
            }
            return new Date().getTime();
    }
    
}
