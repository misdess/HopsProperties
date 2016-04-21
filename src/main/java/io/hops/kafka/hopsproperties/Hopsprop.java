/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.hops.kafka.hopsproperties;

import java.sql.Connection;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author misdess
 */
public class Hopsprop extends Properties {

    static String keyStore;
    static String trustStore;

    //what is the userName, 
    public static Hopsprop create(String userName) {

        String databaseHost = null;
        String databasePort = null;
        String dbUserName = null;
        String dbPassword = null;

        PreparedStatement prepStatement;
        Connection conn = null;

        Byte userKey = null;
        Byte userCert = null;
        //read the mysql reference here
        BufferedReader br;
        String nextLine;
        String split[];

        String filePath = null;
        String dirPath = "";

        Hopsprop hops = new Hopsprop();

        /*
        if HADOOP_CONF_DIR is set
            path is HADOOP_CONF_DIR/ndb.props
        if HADOOP_HOME is set
            path is HADOOP_HOME/etc/hadoop/ndb.props
        else
            path is /srv/hadoop/etc/hadoop/ndb.props       
         */
        if ((dirPath = System.getenv("HADOOP_CONF_DIR")) != null) {
            filePath = dirPath + "/ndb.props";
        } else if ((dirPath = System.getenv("HADOOP_HOME")) != null) {
            filePath = dirPath + "etc/ndb.props";
        } else {
            filePath = "path to mysql properties file";
        }

        try {
            //read the mysql database properties
            br = new BufferedReader(new FileReader(filePath));
            while ((nextLine = br.readLine()) != null) {
                split = nextLine.split("=");
                if (split[0].contains("mysqlserver.host")) {
                    databaseHost = split[1].trim();
                }
                if (split[0].contains("mysqlserver.port")) {
                    databasePort = split[1].trim();
                }
                if (split[0].contains("mysqlserver.username")) {
                    dbUserName = split[1].trim();
                }
                if (split[0].contains("mysqlserver.password")) {
                    dbPassword = split[1].trim();
                    break;
                }
            }

            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://"+databaseHost+":" + databasePort + "/hopsworks", dbUserName, dbPassword);

            prepStatement = conn.prepareStatement("SELECT * from users where username=?");
            prepStatement.setString(1, userName);
            String userId = null;
            ResultSet resutlSet = prepStatement.executeQuery();

            if (resutlSet.next()) {
                userId = resutlSet.getString("uid");
            }

            prepStatement = conn.prepareStatement("SELECT * from user_certs where user_id=?");
            prepStatement.setString(1, userId);
            resutlSet = prepStatement.executeQuery();

            if (resutlSet.next()) {
                userKey = resutlSet.getByte("user_key");
                userCert = resutlSet.getByte("user_cert");
            }

            String username = System.getProperty("user.name");
            //make the files permission 600

            File file1 = File.createTempFile(username + ".keystore", "jks", new File("/tmp"));
            FileOutputStream stream = new FileOutputStream(file1);
            stream.write("misganu".getBytes());
            stream.flush();
            stream.close();
            hops.setProperty("keyStore", "/tmp/" + file1.getName());
            //file1.deleteOnExit();

            //truststore location
            File file2 = File.createTempFile(username + ".truststore", "jks", new File("/tmp"));
            stream = new FileOutputStream(file2);
            stream.write("dessalegn".getBytes());
            stream.flush();
            stream.close();
            hops.setProperty("trustStore", "/tmp/" + file2.getName());
            //file2.deleteOnExit();

        } catch (SQLException | ClassNotFoundException ex) {
            Logger.getLogger(Hopsprop.class.getName()).log(Level.SEVERE, null, ex);
        } catch (FileNotFoundException ex) {

        } catch (IOException ex) {
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ex) {
                    Logger.getLogger(Hopsprop.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }

        return hops;
    }
}
