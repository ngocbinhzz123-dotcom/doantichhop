package com.doan.luongmonhoc;

import com.doan.util.DBConnetor;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.Statement;

public class MonHocSqlProducer {

    private final static String QUEUE_NAME = "monhoc_queue";
    
    // (Đã xóa URL, USER, PASS)

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection rabbitConnection = factory.newConnection();
             Channel rabbitChannel = rabbitConnection.createChannel();
             
             java.sql.Connection sqlConnection = DBConnetor.getConnectionNguon2(); // Đã sửa
             Statement stmt = sqlConnection.createStatement()) {

            rabbitChannel.queueDeclare(QUEUE_NAME, true, false, false, null);

            String sqlSelect = "SELECT MaMon, TenMon, SoTinChi, HeSo FROM Bang_MonHoc";
            ResultSet rs = stmt.executeQuery(sqlSelect);

            while (rs.next()) {
                String payload = String.join(",", 
                    rs.getString("MaMon"),
                    rs.getString("TenMon"),
                    rs.getString("SoTinChi"), 
                    rs.getString("HeSo")
                );
                String message = "SQL," + payload;

                rabbitChannel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
                System.out.println(" [SQL-MH] Đã gửi: '" + rs.getString("MaMon") + "'");
            }
            System.out.println("Hoàn tất gửi dữ liệu MonHoc từ SQL Nguồn 2!");
        }
    }
}