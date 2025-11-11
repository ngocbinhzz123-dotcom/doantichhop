package com.doan.luonglophocphan;

import com.doan.util.DBConnetor;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.Statement;

public class LopHocPhanSqlProducer {

    private final static String QUEUE_NAME = "lophocphan_queue";
    
    // (Đã xóa URL, USER, PASS)

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection rabbitConnection = factory.newConnection();
             Channel rabbitChannel = rabbitConnection.createChannel();
             
             java.sql.Connection sqlConnection = DBConnetor.getConnectionNguon2(); // Đã sửa
             Statement stmt = sqlConnection.createStatement()) {

            rabbitChannel.queueDeclare(QUEUE_NAME, true, false, false, null);

            String sqlSelect = "SELECT MaLopHP, MaMon, MaGV, HocKy, NamHoc FROM Bang_LopHocPhan";
            ResultSet rs = stmt.executeQuery(sqlSelect);

            while (rs.next()) {
                String payload = String.join(",", 
                    rs.getString("MaLopHP"),
                    rs.getString("MaMon"),
                    rs.getString("MaGV"),
                    rs.getString("HocKy"),
                    rs.getString("NamHoc")
                );
                String message = "SQL," + payload; 

                rabbitChannel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
                System.out.println(" [SQL-LHP] Đã gửi: '" + rs.getString("MaLopHP") + "'");
            }
            System.out.println("Hoàn tất gửi dữ liệu LopHocPhan từ SQL Nguồn 2!");
        }
    }
}