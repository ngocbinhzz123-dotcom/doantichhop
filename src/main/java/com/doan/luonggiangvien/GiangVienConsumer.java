package com.doan.luonggiangvien;

import com.doan.util.DBConnetor; // Đã import
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

public class GiangVienConsumer {

    private final static String QUEUE_NAME = "giangvien_queue";
    
    // (Đã xóa URL, USER, PASS)
    
    private static final String SQL_INSERT_STAGING = "INSERT INTO STAGING_GIANGVIEN " +
        "(MaGV, HoTen, MaKhoa, Email, SDT, NguonDuLieu, TrangThaiQC) " +
        "VALUES (?, ?, ?, ?, ?, ?, 'PENDING')";

    public static void main(String[] argv) throws Exception {
        
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection rabbitConnection = factory.newConnection();
        Channel channel = rabbitConnection.createChannel();
        java.sql.Connection sqlConnection = null;

        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        System.out.println(" [*] Đang chờ message [GiangVien] để lưu vào STAGING...");

        try {
            sqlConnection = DBConnetor.getConnectionDich(); // Đã sửa
            System.out.println("Ket noi DB Dich [QuanLyDiemSV] thanh cong!");
            
            final java.sql.Connection finalSqlConnection = sqlConnection;

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                
                try (PreparedStatement ps = finalSqlConnection.prepareStatement(SQL_INSERT_STAGING)) {
                    
                    String nguon = message.substring(0, 3);
                    String payloadStr = message.substring(4);
                    String[] data = payloadStr.split(",");
                    
                    ps.setString(1, data[0]); // MaGV
                    ps.setString(2, data[1]); // HoTen
                    ps.setString(3, data[2]); // MaKhoa

                    if (nguon.equals("CSV")) { // Message từ CSV (có 5 cột)
                        ps.setString(4, data[3]); // Email
                        ps.setString(5, data[4]); // SDT
                    } else { // Message từ SQL (chỉ có 3 cột)
                        ps.setNull(4, Types.VARCHAR); // Email = NULL
                        ps.setNull(5, Types.VARCHAR); // SDT = NULL
                    }
                    ps.setString(6, nguon);   // NguonDuLieu
                    
                    ps.executeUpdate();
                    System.out.println("    -> Đã lưu vào STAGING_GIANGVIEN: " + data[0]);

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