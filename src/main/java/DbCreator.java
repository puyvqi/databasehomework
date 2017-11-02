import com.mysql.jdbc.*;
import jxl.Cell;
import jxl.Sheet;
import jxl.Workbook;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

//import jxl.
public class DbCreator {
    public static Connection cn=null;
    public static Map<String,String> phonebook=null;
    public static void main(String[] args){
        DbCreator.testDataBase();
        batchinsert( DbCreator.xls());
        phonebook=DbCreator.readphone();
        DbCreator.brokeup();

    }
    public static void testDataBase(){
        Connection cnn=null;
        try{
            Class.forName("com.mysql.jdbc.Driver");
        }catch (ClassNotFoundException e){e.printStackTrace();}
        try{
            cnn=DriverManager.getConnection("jdbc:mysql://localhost/?serverTimezone=UTC&useSSL=false&useUnicode=true&characterEncoding=utf-8"
                    ,"root","");
            Statement stmt=cnn.createStatement();
            stmt.execute("create DATABASE if not exists `databasehomework`");
            stmt.execute("use databasehomework");
            stmt.execute("DROP TABLE IF EXISTS `allocatestrategy`");
            stmt.execute("CREATE TABLE if not exists `allocatestrategy`(" +
                    "school VARCHAR(64),id VARCHAR(64) primary key,name VARCHAR(64)," +
                    "sex VARCHAR(64) ," + "area VARCHAR(64),dormitory VARCHAR(64),fee FLOAT )ENGINE=InnoDB DEFAULT CHARSET=utf8");
            cn=cnn;
            stmt.close();
        }catch(SQLException e){
            e.printStackTrace();
        }

    }
    public static ArrayList<String[]> xls(){
        ArrayList<String[]> sh=new ArrayList<String[]>();
        try{
            InputStream in=new FileInputStream(".\\data\\分配方案.xls");
            //BufferedReader r=new BufferedReader(new InputStreamReader(in));
            Workbook rwb=Workbook.getWorkbook(in);
            //System.out.print(r.readLine());
            Sheet sheet=rwb.getSheet(0);
            for(int i=0;i!=sheet.getRows();i++){
                if(i==0)continue;
                String[] str=new String[7];
                for(int j=0;j!=7;j++){
                    Cell cell=sheet.getCell(j,i);
                    str[j]=cell.getContents();
                    //System.out.print(cell.getContents());
                }
                sh.add(str);
                //System.out.println();
            }

        }catch(FileNotFoundException e){e.printStackTrace();}
        catch (IOException e2){e2.printStackTrace();}
        catch(jxl.read.biff.BiffException e3){e3.printStackTrace();}
        return sh;
    }
    public static void batchinsert(ArrayList<String[]> sh){
        if(cn==null){
            System.err.println("cn is null");
            return;
        } else {
            try{
                cn.setAutoCommit(false);
                PreparedStatement ps=cn.prepareStatement("insert into allocatestrategy values(?,?,?,?,?,?,?)");
                //ps.setString(1);
                for(int i=0;i!=sh.size();i++){

                    ps.setString(1,sh.get(i)[1-1]);
                    ps.setString(2,sh.get(i)[2-1]);
                    ps.setString(3,sh.get(i)[3-1]);
                    ps.setString(4,sh.get(i)[4-1]);
                    ps.setString(5,sh.get(i)[5-1]);
                    ps.setString(6,sh.get(i)[6-1]);
                    ps.setFloat(7,Float.parseFloat(sh.get(i)[7-1]));
                    ps.addBatch();
                    if(i%100000==0&&i!=0){
                        ps.executeBatch();
                        System.out.println(i);
                    }
                }
                ps.executeBatch();
                ps.close();
                cn.commit();
            }catch(SQLException E){
                E.printStackTrace();
            }

        }
    }
    public static Map<String,String>readphone(){
        TreeMap<String,String> contect=new TreeMap<String, String>();
        String[] pair=null;
        if(cn==null){
            System.err.println("cn is null when geting phonenum");
            return null;
        }else {
            try{
                String s;
                BufferedReader br=new BufferedReader(new InputStreamReader(new FileInputStream(".\\data\\电话.txt")));
                for (int i=0;(s=br.readLine())!=null;i++){
                    if(i==0)continue;
                    //System.out.println(s);
                    pair=s.split(";");
                    contect.put(pair[0],pair[1]);
                    //System.out.println(pair[0]+":"+contect.get(pair[0]));

                }
            }catch (FileNotFoundException e){e.printStackTrace();}
            catch (IOException e2){e2.printStackTrace();}

        }
        return contect;
    }
    public static void brokeup(){
        ArrayList<String[]> dorentity=new ArrayList<String[]>();
        if(cn==null){
            System.err.println("cn is null when breaking up");
        }else{
            try {
                String dorm=null;
                String area=null;
                float fee=0;
                String sex=null;
                Statement stmt=cn.createStatement();
                ResultSet rs=stmt.executeQuery(" select distinct dormitory,area,fee,sex from allocatestrategy");
                while(rs.next()){
                    dorm=rs.getString(1);
                    area=rs.getString(2);
                    fee=rs.getFloat(3);
                    sex=rs.getString(4);
                    dorentity.add(new String[]{dorm,area,DbCreator.phonebook.get(dorm),sex,String.valueOf(fee)});
                    //System.out.println(dorm+":"+area+":"+String.valueOf(fee)+":"+DbCreator.phonebook.get(dorm)+":"+sex);
                }
                stmt.execute("DROP TABLE if EXISTS `dormitory`");
                stmt.execute("CREATE TABLE IF NOT EXISTS `dormitory`(dormname VARCHAR(64)," +
                        "area VARCHAR(64),phone VARCHAR(64),sex VARCHAR(64) ,fee FLOAT ,PRIMARY KEY (dormname,sex))ENGINE=InnoDB DEFAULT CHARSET=utf8");

                stmt.close();
                PreparedStatement ps=cn.prepareStatement("INSERT INTO `dormitory`VALUES(?,?,?,?,?)");
                long st=System.currentTimeMillis();
                for(int i=0;i!=dorentity.size();i++){
                    ps.setString(1,dorentity.get(i)[0]);
                    ps.setString(2,dorentity.get(i)[1]);
                    ps.setString(3,dorentity.get(i)[2]);
                    ps.setString(4,dorentity.get(i)[3]);
                    ps.setFloat(5,Float.parseFloat(dorentity.get(i)[4]));
                    ps.addBatch();
                    if(i%100000==0&&i!=0){
                        ps.executeBatch();
                    }
                }
                ps.executeBatch();
                cn.commit();
                long en=System.currentTimeMillis();
                System.out.println("宿舍表插入时间："+(en-st)+"微秒");
                ps.close();
                stmt=cn.createStatement();
                ResultSet studentinfo=stmt.executeQuery("SELECT school,id,`name`,sex,dormitory FROM allocatestrategy");
                ArrayList<String[]> stu=new ArrayList<String[]>();
                while (studentinfo.next()){
                    stu.add(new String[]{studentinfo.getString(1),
                            studentinfo.getString(2),
                            studentinfo.getString(3),
                            studentinfo.getString(4),
                            studentinfo.getString(5)});
                }
                stmt.execute("DROP TABLE IF EXISTS `student`");
                stmt.execute("CREATE TABLE if not exists `student`(" +
                        "school VARCHAR(64),id VARCHAR(64) primary key,`name` VARCHAR(64)," +
                        "sex VARCHAR(64) ,dormitory VARCHAR(64) )ENGINE=InnoDB DEFAULT CHARSET=utf8");
                stmt.close();
                ps=cn.prepareStatement("INSERT INTO `student` VALUES(?,?,?,?,?)");
                long starttime=System.currentTimeMillis();
                for(int i=0;i!=stu.size();i++){
                    ps.setString(1,stu.get(i)[0]);
                    ps.setString(2,stu.get(i)[1]);
                    ps.setString(3,stu.get(i)[2]);
                    ps.setString(4,stu.get(i)[3]);
                    ps.setString(5,stu.get(i)[4]);
                    ps.addBatch();
                    if(i%100000==0&&i!=0){
                        ps.executeBatch();
                    }
                }
                ps.executeBatch();
                cn.commit();
                long endtime=System.currentTimeMillis();
                System.out.println("插入学生表的时间："+(endtime-starttime)+"微秒");
                ps.close();
                //ps.setString();
            }catch (SQLException e){e.printStackTrace();}


        }
    }
}
