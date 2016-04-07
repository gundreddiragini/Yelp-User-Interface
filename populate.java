import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.sql.*;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class populate {
static final String host="dbserver.engr.scu.edu";
static final String port="1521";
static final String dbName = "db11g";
static final String Username="yelp";
static final String pass="mypassword";
static String dbURL = "jdbc:oracle:thin:@" + host + ":" + port + ":" + dbName;
 static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
   static final String DB_URL = "jdbc:mysql://localhost/yelp";
   
public static void main(String[] args){
File file=new File(args[0]);
File file1=new File(args[1]);
File file2=new File(args[2]);
new populate(file,file1,file2);
}

    Connection conn=null;
    Statement stmt=null;
    PreparedStatement stmt1=null;
    JSONParser parser=new JSONParser();
 

public populate(File file,File file1,File file2){

               //oc(file);
               //oc1(file1);
               //oc2(file2);
               //oc3(file2);
               oc4(file2);
               }

public void oc(File readfile){
     try{
        FileReader file=new FileReader(readfile);
        BufferedReader br=new BufferedReader(file);
        //Class.forName("oracle.jdbc.driver.OracleDriver");
        Class.forName("com.mysql.jdbc.Driver");
        System.out.println("connecting to database....");
        conn = DriverManager.getConnection(DB_URL,Username,pass);
        //conn=DriverManager.getConnection(dbURL,Username,pass);
        System.out.println("creating statement.....");
        stmt=conn.createStatement();
        String Line;
        int counter = 0;
        String insertTableSQL = "INSERT INTO yelp_user"
                + "(since,cool,funny,useful,review_count,name,user_id,fans,avg_stars,type) VALUES"
                + "(?,?,?,?,?,?,?,?,?,?)";
            stmt1 = conn.prepareStatement(insertTableSQL);

          // br = new BufferedReader(new FileReader("yelp_user.json"));

           while ((Line = br.readLine()) != null) {
               
               counter ++;
               
                   Object obj = parser.parse(Line);
                   JSONObject jsonObject = (JSONObject) obj;

                   String since = (String) jsonObject.get("yelping_since");
                   long review_count = (Long) jsonObject.get("review_count");
                   JSONObject jsonObject1 = (JSONObject) jsonObject.get("votes");
                   long cool = (Long) jsonObject1.get("cool");
                   long funny = (Long) jsonObject1.get("funny");
                   long useful = (Long) jsonObject1.get("useful");
                   String name = (String) jsonObject.get("name");
                   String user_id = (String) jsonObject.get("user_id");
                   long fans = (Long) jsonObject.get("fans");
                   double avg_stars = (Double) jsonObject.get("average_stars");
                   String type = (String) jsonObject.get("type");
                       
                   

                   

                   stmt1.setString(1, since);
                   stmt1.setLong(2, cool);
                   stmt1.setLong(3, funny);
                   stmt1.setLong(4, useful);
                   stmt1.setLong(5, review_count);
                   stmt1.setString(6, name);
                   stmt1.setString(7, user_id);
                   stmt1.setLong(8, fans);
                   stmt1.setDouble(9, avg_stars);
                   stmt1.setString(10, type);
                   // execute insert SQL statement
                   stmt1.execute();

                   //System.out.println("Record is inserted into yelp_user table!");

           }
        
    }catch(SQLException se){
        se.printStackTrace();
    }catch(Exception e){
        e.printStackTrace();
    }finally{
        // nothing we can do
         try{
            if(stmt!=null){stmt.close();}
            if(stmt1!=null){stmt.close();}
            if(conn!=null)
               conn.close();
         }catch(SQLException se){
            se.printStackTrace();
         }//end finally try
      }//end try
      System.out.println("Goodbye!");
    }
 
public void oc1(File readfile){
try{
        FileReader file=new FileReader(readfile);
        BufferedReader br=new BufferedReader(file);
        //Class.forName("oracle.jdbc.driver.OracleDriver");
        Class.forName("com.mysql.jdbc.Driver");
        System.out.println("connecting to database....");
        //conn=DriverManager.getConnection(dbURL,Username,pass);
        conn = DriverManager.getConnection(DB_URL,Username,pass);
        System.out.println("creating statement.....");
        stmt=conn.createStatement();
        String Line;
        int counter = 0;
        String insertTableSQL = "INSERT INTO yelp_review"
                + "(ruser_id,rcool,rfunny,ruseful,revdate,review_id,business_id,text,stars,rtype) VALUES"
                + "(?,?,?,?,?,?,?,?,?,?)";
            stmt1 = conn.prepareStatement(insertTableSQL);

          // br = new BufferedReader(new FileReader("yelp_review.json"));

           while ((Line = br.readLine()) != null) {

               
               counter ++;
               
                   Object obj = parser.parse(Line);
                   JSONObject jsonObject = (JSONObject) obj;
                   String ruser_id = (String) jsonObject.get("user_id");
                   JSONObject jsonObject1 = (JSONObject) jsonObject.get("votes");
                   long rcool = (Long) jsonObject1.get("cool");
                   long rfunny = (Long) jsonObject1.get("funny");
                   long ruseful = (Long) jsonObject1.get("useful");
                       String revdate = (String) jsonObject.get("revdate");
                   String review_id = (String) jsonObject.get("review_id");
                   String business_id = (String) jsonObject.get("business_id");
                   String text = (String) jsonObject.get("text");
                
                      long stars = (((Long) jsonObject.get("stars")).intValue());
              
                   String rtype = (String) jsonObject.get("type");
                       
         
                   stmt1.setString(1, ruser_id);
                   stmt1.setLong(2, rcool);
                   stmt1.setLong(3, rfunny);
                   stmt1.setLong(4, ruseful);
                   stmt1.setString(5, revdate);
                   stmt1.setString(6, review_id);
                   stmt1.setString(7, business_id);
                   stmt1.setString(8, text);
                   stmt1.setLong(9, stars);
                   stmt1.setString(10, rtype);
                   // execute insert SQL statement
                   stmt1.execute();

                   //System.out.println("Record is inserted into yelp_review table!");

           }
        
    }catch(SQLException se){
        se.printStackTrace();
    }catch(Exception e){
        e.printStackTrace();
    }finally{
        // nothing we can do
         try{
            if(stmt!=null){stmt.close();}
            if(stmt1!=null){stmt.close();}
            if(conn!=null)
               conn.close();
         }catch(SQLException se){
            se.printStackTrace();
         }//end finally try
      }//end try
      System.out.println("Goodbye!");
    }
    


public void oc2(File readfile){
try{
        FileReader file=new FileReader(readfile);
        BufferedReader br=new BufferedReader(file);
        //Class.forName("oracle.jdbc.driver.OracleDriver");
        Class.forName("com.mysql.jdbc.Driver");
        System.out.println("connecting to database....");
        //conn=DriverManager.getConnection(dbURL,Username,pass);
        conn = DriverManager.getConnection(DB_URL,Username,pass);
        System.out.println("creating statement.....");
        stmt=conn.createStatement();
        String Line;
        int counter = 0;
        String insertTableSQL = "INSERT INTO yelp_business"
                + "(business_id,address,city,state,open,bname,reviewcount,latitude,longitude,stars,type) VALUES"
                + "(?,?,?,?,?,?,?,?,?,?,?)";
            stmt1 = conn.prepareStatement(insertTableSQL);

          // br = new BufferedReader(new FileReader("yelp_business.json"));

           while ((Line = br.readLine()) != null) {

               
               counter ++;
               
                   Object obj = parser.parse(Line);
                   JSONObject jsonObject = (JSONObject) obj;

                   String business_id = (String) jsonObject.get("business_id");
                   String address = (String) jsonObject.get("full_address");
                 
                   String city = (String) jsonObject.get("city");
                   String state = (String) jsonObject.get("state");
                   Boolean open = (Boolean) jsonObject.get("open");
                   String bname = (String) jsonObject.get("name");
                   long reviewcount = (Long) jsonObject.get("review_count");
                  double latitude = (Double) jsonObject.get("latitude");
                 double longitude = (Double) jsonObject.get("longitude");
                   double stars = (Double) jsonObject.get("stars");
                   String type = (String) jsonObject.get("type");
                       
                   

                   

                   stmt1.setString(1, business_id);
                   stmt1.setString(2, address);
                   stmt1.setString(3, city);
                   stmt1.setString(4, state);
                   stmt1.setBoolean(5, open);
                   stmt1.setString(6, bname);
                   stmt1.setLong(7, reviewcount);
            stmt1.setDouble(8, latitude);  
            stmt1.setDouble(9, longitude);                               
                   stmt1.setDouble(10, stars);
                   stmt1.setString(11, type);
                   // execute insert SQL statement
                   stmt1.execute();

                   //System.out.println("Record is inserted into yelp_user table!");

           }
        
    }catch(SQLException se){
        se.printStackTrace();
    }catch(Exception e){
        e.printStackTrace();
    }finally{
        // nothing we can do
         try{
            if(stmt!=null){stmt.close();}
            if(stmt1!=null){stmt.close();}
            if(conn!=null)
               conn.close();
         }catch(SQLException se){
            se.printStackTrace();
         }//end finally try
      }//end try
      System.out.println("Goodbye!");
    }
    


public void oc3(File readfile){
try{
        FileReader file=new FileReader(readfile);
        BufferedReader br=new BufferedReader(file);
        //Class.forName("oracle.jdbc.driver.OracleDriver");
        Class.forName("com.mysql.jdbc.Driver");
        System.out.println("connecting to database....");
        //conn=DriverManager.getConnection(dbURL,Username,pass);
        conn = DriverManager.getConnection(DB_URL,Username,pass);
        System.out.println("creating statement.....");
        stmt=conn.createStatement();
        String Line;
        int counter = 0;
   
   String insertTableSQL = "INSERT INTO Attributes" + "(business_id, attribute, value) VALUES"     + "(?,?,?)";
                           
                                                                    
                                                                    
                           stmt1 = conn.prepareStatement(insertTableSQL);       
                     
   
  
          // br = new BufferedReader(new FileReader("yelp_user.json"));

           while ((Line = br.readLine()) != null) {
                 Object obj1, obj2;

               
               counter ++;
               
                   Object obj = parser.parse(Line);
                   JSONObject jsonObject = (JSONObject) obj;

                   Object temporaryObject = (Object) jsonObject.get("attributes");
                              Object id = (Object) jsonObject.get("business_id");
                                                                  
                                    
                                    String trail=temporaryObject.toString();
                                    
                                 //System.out.println(trail);   //prints the value of the JSOn key named="attributes"
                                    obj1=parser.parse(trail);      //parsing the value of the JSOn key named="attributes" to get more JSON pairs that are available
                                 
                                 obj2=parser.parse(obj1.toString());   
                                    
                                    JSONObject jsonObject1 = (JSONObject) obj2;
                                    
                                    
                                    String[] storingAllAttributeKeys=new String[100];
                                    String[] storingAllAttribute=new String[100];
                                    String[] storingAllAttribute2=new String[100];
                                    String[] storingAll=new String[100];
                                    int inner=0,coutingAttributes=0,countingKeys=0;
                                    Set <String> vals=jsonObject1.keySet(); //key set in attributes
                                    int noofpkeys=0;
                                    
                                    Object[] temporaryObject2 =new Object[100];
                                    Object[] temporaryObject1=new Object[100];
                                    Object[] tempJObjects=new Object[100];
                                    for (String s : vals) {
                                   // System.out.println(s);  } // prints all the keys in the JSON key named
                                    temporaryObject1[noofpkeys] = (Object) jsonObject1.get(s);
                                    storingAllAttribute[noofpkeys]=s;
                                    noofpkeys++;
                                    }
                                   //String[] storingAllAttributeKeys;
                                  //  int inner=0,coutingKeys=0;
                                    
                                    for(int y=0;y<noofpkeys;y++)
                                    {
                                    if(temporaryObject1[y] instanceof JSONObject)
                                    {
                                    
                                    tempJObjects[inner]=temporaryObject1[y];
                                    inner++;
                                    }
                                    else{
                                    storingAll[countingKeys]=storingAllAttribute[y];
                                    storingAllAttributeKeys[countingKeys]=temporaryObject1[y].toString();
                                    countingKeys++;
                                    }
                                    }
                                    
                                   int outer=inner;
                                    inner=0;
                                    
                                    for(int yy=0;yy<outer;yy++)
                                    {
                                    JSONObject jsonObject2 = (JSONObject) tempJObjects[yy];
                                    Set <String> vals1=jsonObject2.keySet();
                                    for (String s1 : vals1) {
                                   // System.out.println(s);   
                                   temporaryObject2[inner] = (Object) jsonObject2.get(s1);
                                    storingAllAttribute2[inner]=s1;
                                    inner++;
                                    }
                                    for(int zz=0;zz<inner;zz++)
                                    {
                                    if(zz==inner-1)
                                    inner=0;
                                    storingAll[countingKeys]=storingAllAttribute2[zz];
                                    storingAllAttributeKeys[countingKeys]=temporaryObject2[zz].toString();
                                   countingKeys++;
                                    
                                    }
                                    }
                            for(int please=0;please<countingKeys;please++){
                                   // System.out.println(storingAll[please]+" ========== "+storingAllAttributeKeys[please]);
                                    
                                    
                                                         

                       stmt1.setString(1, id.toString());
                       stmt1.setString(2, storingAll[please]);
                       stmt1.setString(3, storingAllAttributeKeys[please]);
              
              stmt1.execute();
            }  
         
       }
  }catch(SQLException se){
        se.printStackTrace();
    
  }catch(Exception e){
        e.printStackTrace();
    }finally{
        // nothing we can do
         try{
            if(stmt!=null){stmt.close();}
            if(stmt1!=null){stmt.close();}
            if(conn!=null)
               conn.close();
         }catch(SQLException se){
            se.printStackTrace();
         }//end finally try
      }//end try
      System.out.println("Goodbye!");
    }
    


public void oc4(File readfile){
try{
        FileReader file=new FileReader(readfile);
        BufferedReader br=new BufferedReader(file);
        //Class.forName("oracle.jdbc.driver.OracleDriver");
        Class.forName("com.mysql.jdbc.Driver");
        System.out.println("connecting to database....");
        //conn=DriverManager.getConnection(dbURL,Username,pass);
        conn = DriverManager.getConnection(DB_URL,Username,pass);
        System.out.println("creating statement.....");
        stmt=conn.createStatement();
        String Line;
        int counter = 0;
        String insertTableSQL = "INSERT INTO categories"+ "(business_id, category) VALUES"+ "(?,?)";
                               
                                
                                               stmt1 = conn.prepareStatement(insertTableSQL);

          // br = new BufferedReader(new FileReader("yelp_user.json"));

           while ((Line = br.readLine()) != null) {

               counter ++;
               
               
                   Object obj = parser.parse(Line);
                   JSONObject jsonObject = (JSONObject) obj;

                 //String since = (String) jsonObject.get("categories");
                 Object since = (Object) jsonObject.get("categories");
                                    
                                    Object id = (Object) jsonObject.get("business_id");
                                    String trail=since.toString();
                                    String[] vals=trail.split("\"");
                                    for(int j=1;j<vals.length;j=j+2)
                                    {
                                    //System.out.println(vals[j]);
                                        stmt1.setString(1, id.toString());
                                              stmt1.setString(2, vals[j]);
                       stmt1.execute();
              }
       }      
    }catch(SQLException se){
        se.printStackTrace();
    }catch(Exception e){
        e.printStackTrace();
    }finally{
        // nothing we can do
         try{
            if(stmt!=null){stmt.close();}
            if(stmt1!=null){stmt.close();}
            if(conn!=null)
               conn.close();
         }catch(SQLException se){
            se.printStackTrace();
         }//end finally try
      }//end try
      System.out.println("Goodbye!");
    }
    
}