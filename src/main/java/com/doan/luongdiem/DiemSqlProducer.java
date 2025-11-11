package com.doan.luongdiem;

import com.doan.util.ketnoi;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.Statement;

public class DiemSqlProducer {

    private final static String QUEUE_NAME = "diem_queue";
    
    // (Đã xóa URL, USER, PASS)

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection rabbitConnection = factory.newConnection();
             Channel rabbitChannel = rabbitConnection.createChannel();
             
             java.sql.Connection sqlConnection = ketnoi.getConnectionNguon2(); // Đã sửa
             Statement stmt = sqlConnection.createStatement()) {

            rabbitChannel.queueDeclare(QUEUE_NAME, true, false, false, null);

            String sqlSelect = "SELECT MaSV, MaLopHP, DiemPhatBieu, DiemBaiTap, DiemChuyenCan, DiemGiuaKy, DiemDoAn, DiemCuoiKi FROM Bang_Diem";
            ResultSet rs = stmt.executeQuery(sqlSelect);

            while (rs.next()) {
                String payload = String.join(",", 
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
                System.out.println(" [SQL-DIEM] Đã gửi: '" + rs.getString("MaSV") + " - " + rs.getString("MaLopHP") + "'");
            }
            System.out.println("Hoàn tất gửi dữ liệu Diem từ SQL Nguồn 2!");
        }
    }
}