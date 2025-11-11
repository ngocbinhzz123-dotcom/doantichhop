package com.doan.khoa;

import com.doan.util.ketnoi;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class KhoaConsumer {

    private final static String QUEUE_NAME = "khoa_queue";
    
    private static final String SQL_INSERT_STAGING = "INSERT INTO STAGING_KHOA " +
        "(MaKhoa, TenKhoa, NguonDuLieu, TrangThaiQC) " +
        "VALUES (?, ?, ?, 'PENDING')";

    public static void main(String[] argv) throws Exception {
        
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection rabbitConnection = factory.newConnection();
        Channel channel = rabbitConnection.createChannel();
        java.sql.Connection sqlConnection = null;

        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        System.out.println(" [*] Đang chờ message [Khoa] để lưu vào STAGING...");

        try {
            sqlConnection = ketnoi.getConnectionDich(); 
            System.out.println("Ket noi DB Dich [QuanLyDiemSV] thanh cong!");
            
            final java.sql.Connection finalSqlConnection = sqlConnection;

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                
                // Bỏ qua message rỗng
                if (message == null || message.trim().isEmpty()) {
                    System.err.println(" ! Lỗi: Nhận được message rỗng, bỏ qua.");
                    return; 
                }

                try (PreparedStatement ps = finalSqlConnection.prepareStatement(SQL_INSERT_STAGING)) {
                    
                    String nguon = message.substring(0, 3);
                    String payloadStr = message.substring(4);
                    String[] data = payloadStr.split(",");
                    
                    ps.setString(1, data[0]); // MaKhoa
                    ps.setString(2, data[1]); // TenKhoa
                    ps.setString(3, nguon);   // NguonDuLieu
                    
                    ps.executeUpdate();
                    System.out.println("    -> Đã lưu vào STAGING_KHOA: " + data[0]);

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