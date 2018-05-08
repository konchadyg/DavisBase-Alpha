/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author konchady
 */
import davisbase.*;
import java.io.IOException;
public class NewClass {
    public static void main(String[] args) throws IOException
    {
        String qs="UPDATE TABLENAME SET NAME=\"HELLO\" WHERE ROW_ID=1";
        String str1 = qs.substring(0, qs.indexOf("WHERE"));
        String str2 = qs.substring(qs.indexOf("WHERE")+6, qs.length());
        //Database.showTables();
        System.out.println(str1+" "+str2);
        String[] str1arr = str1.split(" ");
        String[] str2arr = str2.split(" ");
        String tableName = str1arr[1];
		String colm = str1arr[3];
		String val = str1arr[4];
		int ordnum = 0;
		
		String[] condcolm = new String[str2arr.length/3];
		String[] oper1 = new String[str2arr.length/3];
		String[] values = new String[str2arr.length/3];
		String[] oper2 = new String[(str2arr.length/3)-1];
		int[] ordinalnum = new int[str2arr.length/3];
		int j = 0;
		for(int i = 0; i < str2arr.length; i+=4){
			condcolm[j] = str2arr[i];
			oper1[j] = str2arr[i+1];
			values[j] = str2arr[i+2];
			if((i+3) < str2arr.length){
				oper2[j] = str2arr[i+3];
			}
			j++;
		}
    }
    
}
