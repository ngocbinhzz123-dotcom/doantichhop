package com.doan.luongsinhvien;

import com.doan.util.DBConnetor; 
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.Statement;

public class SinhVienSqlProducer {

    private final static String QUEUE_NAME = "sinhvien_queue";
    
    // (Đã xóa 3 dòng URL, USER, PASS ở đây)

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost"); // RabbitMQ server

        try (Connection rabbitConnection = factory.newConnection();
             Channel rabbitChannel = rabbitConnection.createChannel();
             
             // 2. GỌI HÀM KẾT NỐI NGUỒN 2 TỪ FILE CHUNG
             java.sql.Connection sqlConnection = DBConnetor.getConnectionNguon2();
             Statement stmt = sqlConnection.createStatement()) {

            rabbitChannel.queueDeclare(QUEUE_NAME, true, false, false, null);

            // Đọc 6 cột từ Bảng Nguồn 2 (không có Email, SDT)
            String sqlSelect = "SELECT MaSV, HoTen, NgaySinh, GioiTinh, MaLop, MaKhoa FROM Bang_SinhVien";
            ResultSet rs = stmt.executeQuery(sqlSelect);

            while (rs.next()) {
                // Xây dựng message (6 cột)
                String message = String.join(",", 
                    rs.getString("MaSV"),
                    rs.getString("HoTen"),
                    rs.getString("NgaySinh"), // Định dạng: YYYY-MM-DD
                    rs.getString("GioiTinh"),
                    rs.getString("MaLop"),
                    rs.getString("MaKhoa")
                );
                
                String messageWithPrefix = "SQL," + message; // Thêm tiền tố Nguồn

                rabbitChannel.basicPublish("", QUEUE_NAME, null, messageWithPrefix.getBytes(StandardCharsets.UTF_8));
                System.out.println(" [SQL-SV] Đã gửi: '" + rs.getString("MaSV") + "'");
            }
            
            System.out.println("Hoàn tất gửi dữ liệu SinhVien từ SQL Nguồn 2!");
        }
    }
}