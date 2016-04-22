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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author misdess
 */
public class Hopsprop extends Properties {

    static String keyStore;
    static String trustStore;
    static Byte keyStorePw;
    static Byte trustStorePw;

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

        String filePath;
        String dirPath;

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
            if (!dirPath.endsWith("/")) {
                dirPath = dirPath.concat("/");
            }
            filePath = dirPath + "ndb.props";
        } else if ((dirPath = System.getenv("HADOOP_HOME")) != null) {
            if (!dirPath.endsWith("/")) {
                dirPath = dirPath.concat("/");
            }
            filePath = dirPath + "etc/ndb.props";
        } else {
            filePath = "path to mysql properties file";
        }

        try {
            //read the mysql database properties
            br = new BufferedReader(new FileReader("/tmp/ndb.props"));
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
            //conn = DriverManager.getConnection("jdbc:mysql://"+databaseHost+":" + databasePort + "/hopsworks", dbUserName, dbPassword);
            conn = DriverManager.getConnection("jdbc:mysql://bbc1.sics.se:13004/hopsworks", dbUserName, dbPassword);

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
                keyStorePw = resutlSet.getByte("keystorepassword");
                trustStorePw = resutlSet.getByte("truststorepassword");
            }

            String username = System.getProperty("user.name");
            File file1 = File.createTempFile(username + ".keystore", ".jks", new File(System.getProperty("user.name")));
            FileOutputStream stream = new FileOutputStream(file1);
            stream.write(userKey);
            stream.flush();
            stream.close();
            hops.setProperty("keystore", System.getProperty("user.name") + file1.getName());
            hops.setProperty("keystore.password", keyStorePw.toString());
            //file1.deleteOnExit();

            //truststore location
            File file2 = File.createTempFile(username + ".truststore", ".jks", new File(System.getProperty("user.name")));
            stream = new FileOutputStream(file2);
            stream.write(userCert);
            stream.flush();
            stream.close();
            hops.setProperty("truststore", System.getProperty("user.name") + file2.getName());
            hops.setProperty("truststore.password", trustStorePw.toString());
            //file2.deleteOnExit();

            //make the files permission 600. this fails when file is in /tmp. how do we solve this.
            Set<PosixFilePermission> perms = new HashSet<>();
            perms.add(PosixFilePermission.OWNER_READ);
            perms.add(PosixFilePermission.OWNER_WRITE);
            Files.setPosixFilePermissions(Paths.get(hops.getProperty("keystore")), perms);
            Files.setPosixFilePermissions(Paths.get(hops.getProperty("truststore")), perms);

            System.out.println("file1 is executable: "+file1.canExecute());
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
