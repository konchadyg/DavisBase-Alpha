
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author konchady
 */
public class RandomAccessTest {
    
    public static void main(String[] args)
    {
        RandomAccessFile rf=null;
        try {
            rf = new RandomAccessFile("test.dat", "rw");
            
            rf.setLength(500);
            rf.seek(22);
            
            
            
            rf.writeInt(3000);
            rf.writeChars(" ");
            rf.writeUTF("Hello");
            
            rf.writeInt(3000);
            rf.writeChars(" ");
            rf.writeUTF("Hello");

            rf.writeInt(3000);
            rf.writeChars(" ");
            rf.writeUTF("Hello");            
            
            rf.seek(22);
            
            
            //Long l = rf.length();
            
            //int x = l.intValue();
            
            //for (int i=0;i<x;i++)
            {
                
                System.out.println(""+rf.readInt());
                System.out.println(""+rf.getFilePointer());
                System.out.println(""+rf.readChar());
                System.out.println(""+rf.getFilePointer());
                System.out.println(""+rf.readUTF());
                System.out.println(""+rf.getFilePointer());
                
                System.out.println("-----------------------");
                
                System.out.println("" + rf.length());
                
                rf.setLength(6000);
                
                System.out.println("" + rf.length());
                
                System.out.println("-----------------------");
                
                //rf.seek(rf.getFilePointer());
                
                System.out.println(""+rf.readInt());
                System.out.println(""+rf.readChar());
                System.out.println(""+rf.readUTF());
                
                System.out.println("-----------------------");
                //rf.seek(rf.length());
                System.out.println(""+rf.readInt());
                System.out.println(""+rf.readChar());
                System.out.println(""+rf.readUTF());
                
                System.out.println("" + rf.length());

                
                //Long l = rf.getFilePointer();
                rf.close();
            }
            
            
            
            
        } catch (FileNotFoundException ex) {
            System.err.println("File not Found. Bummer");
            Logger.getLogger(RandomAccessTest.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            
            System.err.println("Seek Error! ");
            Logger.getLogger(RandomAccessTest.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
}
