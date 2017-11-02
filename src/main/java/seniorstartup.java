import sun.util.calendar.LocalGregorianCalendar;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.sql.Timestamp;

public class seniorstartup {
    public static Connection cn=null;
    static{
        try{
            cn=DriverManager.getConnection("jdbc:mysql://localhost/?serverTimezone=UTC&useSSL=false&useUnicode=true&characterEncoding=utf-8"
                    ,"root","");
            Statement stmt =cn.createStatement();
            stmt.execute("use sharingbike");

            stmt.close();
        }catch (SQLException e){ e.printStackTrace();}
    }

    public static void main(String[] agrs){


        //seniorstartup.init();
        //seniorstartup.batchInsertBike( seniorstartup.readfile("bike"));
        //seniorstartup.batchInsertUser( seniorstartup.readfile("user"));
        //seniorstartup.batchInsertRecord( seniorstartup.readfile("record"));

        seniorstartup.repair();
        String[] s=new String[100];
        for(int i=0;i!=s.length;i++){
            s[i]=String.valueOf(i);
        }
        //seniorstartup.deduceAddr(s);
        //seniorstartup.caculatePayment();
        String st="2015-07-08 06:06:00";
        String et="2015-07-08 06:06:01.11";
        Timestamp t=Timestamp.valueOf(st);
        Timestamp k=Timestamp.valueOf(et);

        System.out.println(k.getTime()-t.getTime());

    }
    /*public static void caculatePayment(){
        try{
            Statement st=cn.createStatement();
            st.execute("ALTER TABLE records " +
                    "ADD price FLOAT ");
        }catch (SQLException E){E.printStackTrace();}
    }*/
    public static void processNewTrans(String s){
        try{
            Statement st=cn.createStatement();
            st.executeUpdate("DELETE FROM `users` WHERE balance<0");
        }catch (SQLException e){e.printStackTrace();}
    }
    public static void repair(){
        try{
            Statement s=cn.createStatement();
            s.executeUpdate("UPDATE `bikes` SET state='repairing' WHERE runningtime>200*3600");
        }catch(SQLException e){
            e.printStackTrace();
        }
    }
    public static void deduceAddr(String[] id){//2.2
        try{
            cn.setAutoCommit(false);
            PreparedStatement ps=cn.prepareStatement(
                    "SELECT ma.userid,ma.destaddr,count(*) as fre " +
                    "FROM records ma " +
                    "WHERE userid=? " +
                    "      AND date_format(ma.desttime,'%H:%i:%s')<='23:59:59' " +
                    "      AND date_format(ma.desttime,'%H:%i:%s')>='18:00:00' " +
                    "GROUP BY  userid,destaddr " +
                    "ORDER BY fre DESC " +
                    "LIMIT 1");
            ArrayList<String[]> guessedAddress=new ArrayList<String[]>();
            for(int i=0;i!=id.length;i++){
                ps.setString(1,id[i]);
                ResultSet rs=ps.executeQuery();
                while(rs.next()){
                    guessedAddress.add(new String[]{rs.getString("userid"),rs.getString("destaddr")});
                    //System.out.println(rs.getString("userid")
                            //+","+rs.getString("destaddr")+":"+rs.getString("fre"));
                }

            }

            ps=cn.prepareStatement("UPDATE `users` SET address=? WHERE id=?");
            for(int i=0;i!=guessedAddress.size();i++){
                ps.setString(1,guessedAddress.get(i)[1]);
                ps.setString(2,guessedAddress.get(i)[0]);
                ps.addBatch();
                if(i%1000==0&&i!=0){
                    ps.executeBatch();
                }
            }
            ps.executeBatch();
            cn.commit();
            ps.close();
        }catch(SQLException e){e.printStackTrace();}

    }
    public static void batchInsertBike(ArrayList<String[]> a){
        try{
            cn.setAutoCommit(false);
            PreparedStatement ps=cn.prepareStatement("INSERT INTO `bikes`(id,runningtime,state)" +
                    "VALUES(?,?,?)");
            for(int i=0;i!=a.size();i++){
                ps.setString(1,a.get(i)[0]);
                ps.setInt(2,0);
                ps.setString(3,"running");
                ps.addBatch();
                if(i%5000==0&&i!=0){
                    ps.executeBatch();
                }
            }
            ps.executeBatch();
            cn.commit();
            ps.close();
                    /*id VARCHAR(64) PRIMARY KEY " +
                    ",runningtime FLOAT NOT NULL DEFAULT 0.0" +
                    ",locatiion VARCHAR(64),state VARCHAR(64)*/

        }catch (SQLException e){e.printStackTrace();}

    }
    public static void batchInsertUser(ArrayList<String[]> a){

        try{
            cn.setAutoCommit(false);
            PreparedStatement ps=cn.prepareStatement("INSERT INTO `users`(id,`name`,phone,balance) VALUES (?,?,?,?)");
            for(int i=0;i!=a.size();i++){
                String[] k=a.get(i);
                ps.setString(1,k[0]);
                ps.setString(2,k[1]);
                ps.setString(3,k[2]);
                ps.setFloat(4,Float.parseFloat(k[3]));
                ps.addBatch();
                if(i%50000==0&&i!=0){
                    ps.executeBatch();
                    System.out.println("user:"+i);
                }

            }
            ps.executeBatch();
            ps.close();
            cn.commit();
        }catch(SQLException e){e.printStackTrace();}
    }/*id VARCHAR(64) PRIMARY KEY ,`name` VARCHAR(64)," +
            "phone VARCHAR(64),address VARCHAR(64),balance FLOAT*/
    public static void batchInsertRecord(ArrayList<String[]> a){
        try{
            PreparedStatement ps=cn.prepareStatement("INSERT INTO `records`VALUES(?,?,?,?,?,?,?)");
            PreparedStatement ps2=cn.prepareStatement("UPDATE `bikes` SET runningtime=runningtime+? WHERE id=?");
            PreparedStatement ps3=cn.prepareStatement("UPDATE `bikes` SET location=? WHERE id=? ");
            PreparedStatement ps4=cn.prepareStatement("UPDATE `users` SET balance=balance-? WHERE id=?");
            cn.setAutoCommit(false);
            for(int i=0;i!=a.size();i++){
                String[] k=a.get(i);
                ps.setString(1,k[0]);//userid
                ps.setString(2,k[1]);//bikeid
                ps.setString(3,k[2]);//srcaddr
                ps.setString(4,k[3]);//srctime
                ps.setString(5,k[4]);//destaddr
                ps.setString(6,k[5]);//desttime
                Float f=seniorstartup.caculatePrice(k[3],k[5]);
                ps.setFloat(7,f);//price
                ps.addBatch();

                ps2.setLong(1,(Timestamp.valueOf(k[5]).getTime()
                        -Timestamp.valueOf(k[3]).getTime())/1000);
                ps2.setString(2,k[1]);
                ps2.addBatch();

                ps3.setString(1,k[4]);
                ps3.setString(2,k[1]);
                ps3.addBatch();

                ps4.setFloat(1,f);
                ps4.setString(2,k[0]);
                ps4.addBatch();
                if(i%100000==0&&i!=0){
                    ps.executeBatch();
                    ps2.executeBatch();
                    ps3.executeBatch();
                    ps4.executeBatch();
                    System.out.println(i);
                }
            }
            ps.executeBatch();
            ps2.executeBatch();
            ps3.executeBatch();
            ps4.executeBatch();
            ps.close();
            ps2.close();
            ps3.close();
            ps3.close();
            cn.commit();

        }catch (SQLException se){se.printStackTrace();}

    }
    public static void init(){
        try{
            Class.forName("com.mysql.jdbc.Driver");
            cn=DriverManager.getConnection("jdbc:mysql://localhost/?serverTimezone=UTC&useSSL=false&useUnicode=true&characterEncoding=utf-8"
                    ,"root","");
            Statement stmt=cn.createStatement();
            stmt.execute("CREATE DATABASE if not EXISTS `sharingbike`");
            stmt.execute("use sharingbike");
            stmt.execute("DROP TABLE IF EXISTS `records`");
            stmt.execute("CREATE TABLE if NOT EXISTS `records`(" +
                    "userid VARCHAR(64),bikeid VARCHAR(64) ," +
                    "srcaddr VARCHAR(64)," +
                    "srctime DATETIME," +
                    "destaddr VARCHAR(64)," +
                    "desttime DATETIME" +
                    ",price FLOAT )ENGINE=InnoDB DEFAULT CHARSET=utf8");
            stmt.execute("DROP TABLE if EXISTS `users`");
            stmt.execute("CREATE TABLE IF NOT EXISTS `users`(" +
                    "id VARCHAR(64) PRIMARY KEY ,`name` VARCHAR(64)," +
                    "phone VARCHAR(64),address VARCHAR(64),balance FLOAT)ENGINE=InnoDB DEFAULT CHARSET=utf8");
            stmt.execute("DROP TABLE IF EXISTS `bikes`");
            stmt.execute("CREATE TABLE IF NOT EXISTS `bikes`(id VARCHAR(64) PRIMARY KEY " +
                    ",runningtime BIGINT NOT NULL DEFAULT 0" +
                    ",location VARCHAR(64),state VARCHAR(64))ENGINE=InnoDB DEFAULT CHARSET=utf8");
            stmt.close();
            //PreparedStatement ps=cn.prepareStatement()
        }catch (ClassNotFoundException e){
            e.printStackTrace();
        }catch (SQLException e2){
            e2.printStackTrace();
        }


    }
    public static ArrayList<String[]> readfile(String filename){
        try{
            ArrayList<String[]> table=new ArrayList<String[]>();
            InputStream fi=new FileInputStream(".\\data\\"+filename+".txt");
            BufferedReader br=new BufferedReader(new InputStreamReader(fi));
            String line=null;
            if(filename=="record"){
                while((line=br.readLine())!=null){
                    String[] k=line.split(";");
                    k[3]=k[3].replaceAll("-"," ").replaceAll("/","-");
                    k[5]=k[5].replaceAll("-"," ").replaceAll("/","-");
                   // System.out.println(k[3]+"->"+k[5]);
                    table.add(k);
                }
            return table;
            }else if(filename=="bike"){
                while ((line=br.readLine())!=null){
                    table.add(new String[]{line,"running","0",""});
                    //System.out.println(line);
                }
                return table;
            }else if(filename=="user"){
                while((line=br.readLine())!=null){
                    String[] s=line.split(";");
                    table.add(s);
                    //System.out.println(line);
                }
                return table;
            }

        }catch (FileNotFoundException e){e.printStackTrace();}
        catch (IOException e2){e2.printStackTrace();}
        return null;

    }
    public static float caculatePrice(String a,String b){
        Timestamp start=Timestamp.valueOf(a);
        Timestamp end=Timestamp.valueOf(b);
        long seconds=(end.getTime()-start.getTime())/1000;
        if(seconds<=1800)return 1;
        else if(seconds<=3600)return (float) 2;
        else if(seconds<=5400)return 3;
        else return 4;
    }
}
