package com.doan.luongsinhvien;

import com.doan.util.DBConnetor; // <-- 1. THÊM IMPORT
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

public class SinhVienConsumer {

    private final static String QUEUE_NAME = "sinhvien_queue";
    
    // (Đã xóa 3 dòng DB_DICH_URL, DB_USER, DB_PASS ở đây)
    
    // Câu lệnh INSERT vào bảng STAGING_SINHVIEN
    private static final String SQL_INSERT_STAGING = "INSERT INTO STAGING_SINHVIEN " +
            "(MaSV, HoTen, NgaySinh, GioiTinh, MaLop, MaKhoa, Email, SDT, NguonDuLieu, TrangThaiQC) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING')";

    public static void main(String[] argv) throws Exception {
        
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        
        Connection rabbitConnection = factory.newConnection();
        Channel channel = rabbitConnection.createChannel();
        
        java.sql.Connection sqlConnection = null; // Khai báo ở ngoài

        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        System.out.println(" [*] Đang chờ message [SinhVien] để lưu vào STAGING...");

        try {
            // 2. GỌI HÀM KẾT NỐI ĐÍCH TỪ FILE CHUNG
            sqlConnection = DBConnetor.getConnectionDich(); 
            
            // Tên DB 'QuanLyDiemSV' được lấy từ file DBConnector
            System.out.println("Kết nối DB Đích [QuanLyDiem_STAGING] thành công!"); 
            
            final java.sql.Connection finalSqlConnection = sqlConnection;

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                
                // Tách message
                String nguon = message.substring(0, 3); // "CSV" hoặc "SQL"
                String payloadStr = message.substring(4); // Dữ liệu
                String[] data = payloadStr.split(",");

                try (PreparedStatement ps = finalSqlConnection.prepareStatement(SQL_INSERT_STAGING)) {
                    
                    ps.setString(1, data[0]); // MaSV
                    ps.setString(2, data[1]); // HoTen
                    ps.setString(3, data[2]); // NgaySinh (lưu dạng thô/String)
                    ps.setString(4, data[3]); // GioiTinh
                    ps.setString(5, data[4]); // MaLop
                    ps.setString(6, data[5]); // MaKhoa

                    if (nguon.equals("CSV")) { // Dữ liệu CSV có 8 cột
                        ps.setString(7, data[6]); // Email
                        ps.setString(8, data[7]); // SDT
                    } else { // Dữ liệu SQL chỉ có 6 cột
                        ps.setNull(7, Types.VARCHAR); // Email = NULL
                        ps.setNull(8, Types.VARCHAR); // SDT = NULL
                    }
                    ps.setString(9, nguon);   // NguonDuLieu
                    
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