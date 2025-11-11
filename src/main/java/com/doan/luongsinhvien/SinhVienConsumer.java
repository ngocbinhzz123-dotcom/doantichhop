package com.doan.luongsinhvien;

import com.doan.util.ketnoi;
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
    
    private static final String SQL_INSERT_STAGING = "INSERT INTO STAGING_SINHVIEN " +
            "(MaSV, HoTen, NgaySinh, GioiTinh, MaLop, MaKhoa, Email, SDT, NguonDuLieu, TrangThaiQC) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING')";

    public static void main(String[] argv) throws Exception {
        
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        
        Connection rabbitConnection = factory.newConnection();
        Channel channel = rabbitConnection.createChannel();
        
        java.sql.Connection sqlConnection = null;

        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        System.out.println(" [*] Đang chờ message [SinhVien] để lưu vào STAGING...");

        try {
            sqlConnection = ketnoi.getConnectionDich(); 
            System.out.println("Kết nối DB Đích [QuanLyDiemSV] thành công!"); 
            
            final java.sql.Connection finalSqlConnection = sqlConnection;

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                
                // Check message rỗng
                if (message == null || message.trim().isEmpty() || message.equals("CSV,")) {
                    System.err.println(" ! Lỗi: Nhận được message rỗng/lỗi, bỏ qua.");
                    return; 
                }

                try (PreparedStatement ps = finalSqlConnection.prepareStatement(SQL_INSERT_STAGING)) {
                    
                    String nguon = message.substring(0, 3);
                    String payloadStr = message.substring(4);
                    String[] data = payloadStr.split(",");
                    
                    // Điền 6 cột chung
                    ps.setString(1, data[0]); // MaSV
                    ps.setString(2, data[1]); // HoTen
                    ps.setString(3, data[2]); // NgaySinh
                    ps.setString(4, data[3]); // GioiTinh
                    ps.setString(5, data[4]); // MaLop
                    ps.setString(6, data[5]); // MaKhoa
                    
                    // [ĐÃ SỬA LOGIC]
                    if (nguon.equals("CSV")) {
                        ps.setString(9, "CSV"); // Set Nguồn
                        if (data.length == 8) {
                            // CSV 8 cột (chuẩn)
                            ps.setString(7, data[6]); // Email
                            ps.setString(8, data[7]); // SDT
                        } else {
                            // CSV 6 cột (bị lỗi, thiếu Email/SDT)
                            ps.setNull(7, Types.VARCHAR); // Email = NULL
                            ps.setNull(8, Types.VARCHAR); // SDT = NULL
                        }
                    } else if (nguon.equals("SQL")) {
                        // SQL 6 cột (chuẩn)
                        ps.setString(9, "SQL"); // Set Nguồn
                        ps.setNull(7, Types.VARCHAR); // Email = NULL
                        ps.setNull(8, Types.VARCHAR); // SDT = NULL
                    } else {
                        // Lỗi (ví dụ: SV0)
                        throw new Exception("NguonDuLieu '" + nguon + "' khong hop le.");
                    }
                    
                    ps.executeUpdate();
                    System.out.println("    -> Đã lưu vào STAGING_SINHVIEN: " + data[0]);

                } catch (SQLException e) {
                    System.err.println(" ! Lỗi SQL khi INSERT vào STAGING: " + e.getMessage());
                } catch (Exception e) {
                    System.err.println(" ! Lỗi xử lý message: " + e.getMessage());
                }
            };

            channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });
            
        } catch (SQLException e) {
            System.err.println("Lỗi nghiêm trọng: Không thể kết nối đến DB Đích.");
            e.printStackTrace();
            
            if (sqlConnection != null) sqlConnection.close();
            if (channel != null) channel.close();
            if (rabbitConnection != null) rabbitConnection.close();
        }
    }
}