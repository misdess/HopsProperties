/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.hops.kafka.hopsproperties;

import java.sql.Connection;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
    public static Hopsprop create(String userName) throws SQLException {

        String databaseHost = null;
        String databasePort = null;
        String dbUserName = null;
        String dbPassword = null;

        Connection conn = null;

        Byte userKey = null;
        Byte userCert = null;
        //read the mysql reference here
        BufferedReader br;
        String nextLine;
        String split[];

        try {
            //read the mysql database properties
            br = new BufferedReader(new FileReader("path to mysql properties file"));
            while ((nextLine = br.readLine()) != null) {
                split = nextLine.split("=");
                if (split[0].contains("mysqlserver.host")) {
                    databaseHost = split[1];
                }

                if (split[0].contains("mysqlserver.port")) {
                    databasePort = split[1];
                }
                if (split[0].contains("mysqlserver.username")) {
                    dbUserName = split[1];
                }
                if (split[0].contains("mysqlserver.password")) {
                    dbPassword = split[1];
                }
            }

        } catch (Exception e) {
            ;
        }
        String databaseUrl = databaseHost + ":" + databasePort + "/hopsworks";

        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://" + databaseUrl, dbUserName, dbPassword);
        } catch (SQLException | ClassNotFoundException ex) {
            ;
        }

        String userId = null;

        try {
            Statement stat = conn.createStatement();
            ResultSet resutlSet = stat.executeQuery("SELECT * from users where username=" + userName);

            if (resutlSet.next()) {
                userId = resutlSet.getString("uid");
            }

            resutlSet = stat.executeQuery("SELECT * from user_cert where user_id=" + userId);
            if (resutlSet.next()) {
                userKey = resutlSet.getByte("user_key");
                userCert = resutlSet.getByte("user_cert");
            }

            try {

                //keystore location
                File file1 = new File("/tmp/kafka/kafka.client.keystore.jks");
                FileOutputStream stream = new FileOutputStream(file1);
                stream.write(userKey);
                stream.close();

                //truststore location
                File file2 = new File("/tmp/kafka/kafka.client.keystore.jks");
                stream = new FileOutputStream(file2);
                stream.write(userCert);
                stream.close();

            } catch (Exception e) {

            }
        } catch (SQLException ex) {
            Logger.getLogger(Hopsprop.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            conn.close();
        }
        keyStore = "/tmp/kafka/kafka.client.keystore.jks";
        trustStore = "/tmp/kafka/kafka.client.keystore.jks";
        return new Hopsprop();
    }
}
