package com.doan.luongmonhoc;



import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MonHocConsumer {

    private final static String QUEUE_NAME = "monhoc_queue";
    
    private static final String DB_DICH_URL = "jdbc:sqlserver://DESKTOP-2EBIQK\\SQLEXPRESS:1433;databaseName=QuanLyDiem_STAGING;encrypt=false;";
    private static final String DB_USER = "sa1";
    private static final String DB_PASS = "chien123";
    
    private static final String SQL_INSERT_STAGING = "INSERT INTO STAGING_MONHOC " +
            "(MaMon, TenMon, SoTinChi, HeSo, NguonDuLiEu, TrangThaiQC) " +
            "VALUES (?, ?, ?, ?, ?, 'PENDING')";

    public static void main(String[] argv) throws Exception {
        
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        
        // [ĐÃ SỬA] Quản lý kết nối RabbitMQ thủ công (bỏ try-with-resources)
        Connection rabbitConnection = factory.newConnection();
        Channel channel = rabbitConnection.createChannel();
        
        // [ĐÃ SỬA] Khai báo kết nối SQL ở ngoài
        java.sql.Connection sqlConnection = null;

        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        System.out.println(" [*] Đang chờ message [MonHoc] để lưu vào STAGING...");

        try {
            // [ĐÃ SỬA] Khởi tạo kết nối SQL (không dùng try-with-resources)
            sqlConnection = DriverManager.getConnection(DB_DICH_URL, DB_USER, DB_PASS);
            System.out.println("Ket noi DB Dich [QuanLyDiem_DoAn] thanh cong!");
            
            // [ĐÃ SỬA] Phải tạo một biến 'final' để dùng trong lambda
            final java.sql.Connection finalSqlConnection = sqlConnection;

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                
                // [ĐÃ SỬA] Dùng kết nối 'finalSqlConnection'
                try (PreparedStatement ps = finalSqlConnection.prepareStatement(SQL_INSERT_STAGING)) {
                    
                    String nguon = message.substring(0, 3); // "CSV" hoặc "SQL"
                    String payloadStr = message.substring(4); // Dữ liệu
                    String[] data = payloadStr.split(",");
                    
                    ps.setString(1, data[0]); // MaMon
                    ps.setString(2, data[1]); // TenMon
                    ps.setString(3, data[2]); // SoTinChi (dạng thô/String)
                    ps.setString(4, data[3]); // HeSo (dạng thô/String)
                    ps.setString(5, nguon);   // NguonDuLieu
                    
                    ps.executeUpdate();
                    System.out.println("    -> Đã lưu vào STAGING_MONHOC: " + data[0]);

                } catch (SQLException e) {
                    // Lỗi INSERT (ví dụ: tên cột sai) sẽ bị bắt ở đây
                    System.err.println(" ! Lỗi SQL khi INSERT vào STAGING: " + e.getMessage());
                } catch (Exception e) {
                    System.err.println(" ! Lỗi xử lý message: " + e.getMessage());
                }
            };
            
            channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });
            
            // Chương trình sẽ KHÔNG kết thúc ở đây, 
            // vì 'sqlConnection' và 'rabbitConnection' vẫn đang được các luồng chạy ngầm sử dụng
            
        } catch (SQLException e) { 
            // Lỗi này chỉ bắt lỗi KẾT NỐI BAN ĐẦU
            System.err.println("Lỗi nghiêm trọng: Không thể kết nối đến DB Đích.");
            e.printStackTrace();
            
            // Dọn dẹp nếu kết nối ban đầu thất bại
            if (sqlConnection != null) sqlConnection.close();
            if (channel != null) channel.close();
            if (rabbitConnection != null) rabbitConnection.close();
        }
    }
}
