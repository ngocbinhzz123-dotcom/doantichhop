package com.doan.luongdiem;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class DiemSqlProducer {

    private final static String QUEUE_NAME = "diem_queue";

    private static final String DB_URL = "jdbc:sqlserver://LAPTOPCUABINH:1433;databaseName=QuanLyDiem_SQL;encrypt=false;";
    private static final String DB_USER = "doan_app";
    private static final String DB_PASS = "123456";

    public static void main(String[] argv) throws Exception {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection rabbitConnection = factory.newConnection();
             Channel rabbitChannel = rabbitConnection.createChannel();
             java.sql.Connection sqlConnection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
             Statement stmt = sqlConnection.createStatement()) {

            rabbitChannel.queueDeclare(QUEUE_NAME, true, false, false, null);

            String sql = "SELECT DiemID, MaSV, MaLopHP, DiemPhatBieu, DiemBaiTap, DiemChuyenCan, DiemGiuaKy, DiemDoAn, DiemCuoiKi FROM Bang_Diem";
            ResultSet rs = stmt.executeQuery(sql);

            while (rs.next()) {
                String payload = String.join(",",
                        rs.getString("DiemID"),
                        rs.getString("MaSV"),
                        rs.getString("MaLopHP"),
                        rs.getString("DiemPhatBieu"),
                        rs.getString("DiemBaiTap"),
                        rs.getString("DiemChuyenCan"),
                        rs.getString("DiemGiuaKy"),
                        rs.getString("DiemDoAn"),
                        rs.getString("DiemCuoiKi")
                );

                String message = "SQL," + payload;
                rabbitChannel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
                System.out.println(" [SQL-DIEM] Đã gửi: '" + rs.getString("DiemID") + "'");
            }

            System.out.println("✅ Hoàn tất gửi dữ liệu Điểm từ SQL!");
        }
    }
}
