package com.doan.khoa;

import com.doan.util.ketnoi;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.Statement;

public class KhoaSqlProducer {

    private final static String QUEUE_NAME = "khoa_queue";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection rabbitConnection = factory.newConnection();
             Channel rabbitChannel = rabbitConnection.createChannel();
             
             java.sql.Connection sqlConnection = ketnoi.getConnectionNguon2();
             Statement stmt = sqlConnection.createStatement()) {

            rabbitChannel.queueDeclare(QUEUE_NAME, true, false, false, null);

            String sqlSelect = "SELECT MaKhoa, TenKhoa FROM Bang_Khoa";
            ResultSet rs = stmt.executeQuery(sqlSelect);

            while (rs.next()) {
                String payload = String.join(",", 
                    rs.getString("MaKhoa"),
                    rs.getString("TenKhoa")
                );
                String message = "SQL," + payload; 

                rabbitChannel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
                System.out.println(" [SQL-KHOA] Đã gửi: '" + rs.getString("MaKhoa") + "'");
            }
            System.out.println("Hoàn tất gửi dữ liệu Khoa từ SQL Nguồn 2!");
        }
    }
}