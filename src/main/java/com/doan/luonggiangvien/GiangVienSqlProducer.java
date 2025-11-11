package com.doan.luonggiangvien;

import com.doan.util.DBConnetor; // Đã import
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.Statement;

public class GiangVienSqlProducer {

    private final static String QUEUE_NAME = "giangvien_queue";
    
    // (Đã xóa URL, USER, PASS)

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection rabbitConnection = factory.newConnection();
             Channel rabbitChannel = rabbitConnection.createChannel();
             
             java.sql.Connection sqlConnection = DBConnetor.getConnectionNguon2(); // Đã sửa
             Statement stmt = sqlConnection.createStatement()) {

            rabbitChannel.queueDeclare(QUEUE_NAME, true, false, false, null);

            // Đọc 3 cột từ Bảng Nguồn 2
            String sqlSelect = "SELECT MaGV, HoTen, MaKhoa FROM Bang_GiangVien";
            ResultSet rs = stmt.executeQuery(sqlSelect);

            while (rs.next()) {
                String payload = String.join(",", 
                    rs.getString("MaGV"),
                    rs.getString("HoTen"),
                    rs.getString("MaKhoa")
                    
                );
                String message = "SQL," + payload; // Thêm tiền tố Nguồn

                rabbitChannel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
                System.out.println(" [SQL-GV] Đã gửi: '" + rs.getString("MaGV") + "'");
            }
            System.out.println("Hoàn tất gửi dữ liệu GiangVien từ SQL Nguồn 2!");
        }
    }
}