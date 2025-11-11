package com.doan.util;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ketnoi {

    private static final String SERVER_NAME = "LAPTOPCUABINH"; 
    private static final boolean IS_NAMED_INSTANCE = false;
    private static final String INSTANCE_NAME = "SQLEXPRESS"; 

    private static final String DB_USER = "doan_app";
    private static final String DB_PASS = "123456";

    private static String buildConnectionString(String databaseName) {
        if (IS_NAMED_INSTANCE) {
            return String.format(
                "jdbc:sqlserver://%s;instanceName=%s;databaseName=%s;encrypt=false;",
                SERVER_NAME, INSTANCE_NAME, databaseName
            );
        } else {
            return String.format(
                "jdbc:sqlserver://%s:1433;databaseName=%s;encrypt=false;",
                SERVER_NAME, databaseName
            );
        }
    }

    // Thông tin DB Đích (QuanLyDiemSV)
    private static final String DB_DICH_NAME = "QuanLyDiem_STAGING";
    private static final String DB_DICH_URL = buildConnectionString(DB_DICH_NAME);

    // Thông tin DB Nguồn 2 (QuanLyDiem_Nguon_2)
    private static final String DB_NGUON_2_NAME = "QuanLyDiem_SQL";
    private static final String DB_NGUON_2_URL = buildConnectionString(DB_NGUON_2_NAME);


    
    public static Connection getConnectionDich() throws SQLException {
        return DriverManager.getConnection(DB_DICH_URL, DB_USER, DB_PASS);
    }

    public static Connection getConnectionNguon2() throws SQLException {
        return DriverManager.getConnection(DB_NGUON_2_URL, DB_USER, DB_PASS);
    }
}
