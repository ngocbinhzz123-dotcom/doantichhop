package com.doan.luonglophocphan;

import com.doan.util.ketnoi;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class LopHocPhanConsumer {

    private final static String QUEUE_NAME = "lophocphan_queue";
    
    // (Đã xóa URL, USER, PASS)
    
    private static final String SQL_INSERT_STAGING = "INSERT INTO STAGING_LOPHOCPHAN " +
        "(MaLopHP, MaMon, MaGV, HocKy, NamHoc, NguonDuLieu, TrangThaiQC) " +
        "VALUES (?, ?, ?, ?, ?, ?, 'PENDING')";

    public static void main(String[] argv) throws Exception {
        
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection rabbitConnection = factory.newConnection();
        Channel channel = rabbitConnection.createChannel();
        java.sql.Connection sqlConnection = null;

        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        System.out.println(" [*] Đang chờ message [LopHocPhan] để lưu vào STAGING...");

        try {
            sqlConnection = ketnoi.getConnectionDich(); // Đã sửa
            System.out.println("Ket noi DB Dich [QuanLyDiemSV] thanh cong!");
            
            final java.sql.Connection finalSqlConnection = sqlConnection;

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                
                try (PreparedStatement ps = finalSqlConnection.prepareStatement(SQL_INSERT_STAGING)) {
                    
                    String nguon = message.substring(0, 3);
                    String payloadStr = message.substring(4);
                    String[] data = payloadStr.split(",");
                    
                    ps.setString(1, data[0]); // MaLopHP
                    ps.setString(2, data[1]); // MaMon
                    ps.setString(3, data[2]); // MaGV
                    ps.setString(4, data[3]); // HocKy
                    ps.setString(5, data[4]); // NamHoc
                    ps.setString(6, nguon);   // NguonDuLieu
                    
                    ps.executeUpdate();
                    System.out.println("    -> Đã lưu vào STAGING_LOPHOCPHAN: " + data[0]);

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