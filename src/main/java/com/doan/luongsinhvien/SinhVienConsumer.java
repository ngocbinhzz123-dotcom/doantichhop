package com.doan.luongsinhvien;

//File: SinhVienConsumer.java
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types; // [ĐÃ THÊM] Cần import Types

public class SinhVienConsumer {

    private final static String QUEUE_NAME = "sinhvien_queue";
    
    // Sửa lại chuỗi kết nối (dùng user/pass và đúng tên DB Đích)
    private static final String DB_DICH_URL = "jdbc:sqlserver://DESKTOP-2EBIQK\\SQLEXPRESS:1433;databaseName=QuanLyDiem_STAGING;encrypt=false;";
    private static final String DB_USER = "sa1";
    private static final String DB_PASS = "chien123";
    
    // Câu lệnh INSERT vào bảng STAGING_SINHVIEN
    private static final String SQL_INSERT_STAGING = "INSERT INTO STAGING_SINHVIEN " +
            "(MaSV, HoTen, NgaySinh, GioiTinh, MaLop, MaKhoa, Email, SDT, NguonDuLieu, TrangThaiQC) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING')";

    public static void main(String[] argv) throws Exception {
        
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        
        // [ĐÃ SỬA] Quản lý kết nối RabbitMQ thủ công
        Connection rabbitConnection = factory.newConnection();
        Channel channel = rabbitConnection.createChannel();
        
        // [ĐÃ SỬA] Khai báo kết nối SQL ở ngoài
        java.sql.Connection sqlConnection = null;

        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        System.out.println(" [*] Đang chờ message [SinhVien] để lưu vào STAGING...");

        try {
            // [ĐÃ SỬA] Khởi tạo kết nối SQL (không dùng try-with-resources)
            sqlConnection = DriverManager.getConnection(DB_DICH_URL, DB_USER, DB_PASS);
            System.out.println("Kết nối DB Đích [QuanLyDiem_DoAn] thành công!");
            
            // [ĐÃ SỬA] Tạo biến final để dùng trong lambda
            final java.sql.Connection finalSqlConnection = sqlConnection;

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                String[] data = message.split(",");

                // [ĐÃ SỬA] Dùng kết nối 'finalSqlConnection'
                try (PreparedStatement ps = finalSqlConnection.prepareStatement(SQL_INSERT_STAGING)) {
                    
                    ps.setString(1, data[0]); // MaSV
                    ps.setString(2, data[1]); // HoTen
                    ps.setString(3, data[2]); // NgaySinh (lưu dạng thô/String)
                    ps.setString(4, data[3]); // GioiTinh
                    ps.setString(5, data[4]); // MaLop
                    ps.setString(6, data[5]); // MaKhoa

                    if (data.length == 8) {
                        // Message từ CSV (có 8 cột)
                        ps.setString(7, data[6]); // Email
                        ps.setString(8, data[7]); // SDT
                        ps.setString(9, "CSV");   // NguonDuLieu
                    } else {
                        // Message từ SQL Nguồn 2 (chỉ có 6 cột)
                        ps.setNull(7, Types.VARCHAR); // Email = NULL
                        ps.setNull(8, Types.VARCHAR); // SDT = NULL
                        ps.setString(9, "SQL");  // NguonDuLieu
                    }
                    
                    ps.executeUpdate();
                    System.out.println("    -> Đã lưu vào STAGING_SINHVIEN: " + data[0]);

                } catch (SQLException e) {
                    System.err.println(" ! Lỗi SQL khi INSERT vào STAGING: " + e.getMessage());
                } catch (Exception e) {
                    System.err.println(" ! Lỗi xử lý message: " + e.getMessage());
                }
            };

            // Bắt đầu lắng nghe
            channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });
            
        } catch (SQLException e) {
            System.err.println("Lỗi nghiêm trọng: Không thể kết nối đến DB Đích.");
            e.printStackTrace();
            
            // Dọn dẹp nếu kết nối ban đầu thất bại
            if (sqlConnection != null) sqlConnection.close();
            if (channel != null) channel.close();
            if (rabbitConnection != null) rabbitConnection.close();
        }
    }
}