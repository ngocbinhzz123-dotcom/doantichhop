package com.doan.khoa;

import com.doan.util.ketnoi;
import com.doan.util.FieldRules;
import com.doan.util.KhoaiFieldRules; 
import com.google.gson.Gson;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.regex.Pattern; 

public class KhoaCR {

    private static final String SQL_SELECT_STAGING = "SELECT * FROM STAGING_KHOA WHERE TrangThaiQC = 'PENDING'";
    private static final String SQL_INSERT_TARGET = "INSERT INTO KHOA_TBL (MaKhoa, TenKhoa) VALUES (?, ?)";
    private static final String SQL_UPDATE_STAGING = "UPDATE STAGING_KHOA SET TrangThaiQC = ? WHERE StagingID = ?";
    private static final String SQL_LOG_ERROR = "INSERT INTO QC_LOG_TBL (StagingID, RuleName, ErrorMessage) VALUES (?, ?, ?)";
    
    private static KhoaiFieldRules cauHinhRule; 

    public static void main(String[] args) {
        try {
            taiCauHinhRule();
            
            try (Connection conn = ketnoi.getConnectionDich()) { 
                conn.setAutoCommit(false); 
                try (Statement stmt = conn.createStatement();
                     ResultSet ketQuaStaging = stmt.executeQuery(SQL_SELECT_STAGING)) {
                    
                    while (ketQuaStaging.next()) {
                        int stagingId = ketQuaStaging.getInt("StagingID");
                        try {
                            // Chạy "checkrules"
                            kiemTraHopLe_Khoa(conn, ketQuaStaging); 
                            
                            // --- XỬ LÝ KHI PASS ---
                            try (PreparedStatement psInsert = conn.prepareStatement(SQL_INSERT_TARGET)) {
                                psInsert.setString(1, ketQuaStaging.getString("MaKhoa"));
                                psInsert.setString(2, ketQuaStaging.getString("TenKhoa"));
                                psInsert.executeUpdate();
                            }
                            capNhatTrangThaiStaging(conn, stagingId, "PASSED");
                            System.out.println("PASSED [Khoa]: StagingID " + stagingId);

                        } catch (Exception loi) { 
                           
                            System.err.println("FAILED [Khoa]: StagingID " + stagingId + " - " + loi.getMessage());
                            ghiNhatKyLoi(conn, stagingId, "KhoaCheckRule", loi.getMessage());
                            capNhatTrangThaiStaging(conn, stagingId, "FAILED");
                        }
                    }
                }
                conn.commit(); 
                System.out.println("Hoan tat kiem tra chat luong [Khoa].");
            }
        } catch (Exception e) { e.printStackTrace(); }
    }
    
    private static void taiCauHinhRule() throws Exception {
        Gson gson = new Gson();
        InputStream is = KhoaCR.class.getClassLoader().getResourceAsStream("khoa_rules.json");
        if (is == null) throw new Exception("Loi: Khong tim thay file 'khoa_rules.json'");
        Reader reader = new InputStreamReader(is);
        cauHinhRule = gson.fromJson(reader, KhoaiFieldRules.class); 
        System.out.println("Da load file config rule [Khoa] thanh cong.");
    }
    
    private static void kiemTraHopLe_Khoa(Connection conn, ResultSet rs) throws Exception {
        FieldRules rule_MaKhoa = cauHinhRule.getMaKhoa();
        FieldRules rule_TenKhoa = cauHinhRule.getTenKhoa();
        
        String maKhoa = rs.getString("MaKhoa");
        String tenKhoa = rs.getString("TenKhoa");
        
        // Check MaKhoa
        if (rule_MaKhoa.isCheckNull() && (maKhoa == null || maKhoa.isEmpty())) throw new Exception("Loi: MaKhoa bi rong (NULL).");
        if (maKhoa.length() > rule_MaKhoa.getMaxLength()) throw new Exception("Loi: MaKhoa qua dai (Max: " + rule_MaKhoa.getMaxLength() + ").");
        if (rule_MaKhoa.isCheckDuplicate() && kiemTraMaKhoaDaTonTai(conn, maKhoa)) throw new Exception("Loi: MaKhoa '" + maKhoa + "' da ton tai.");
        if (rule_MaKhoa.getRegex() != null && !Pattern.matches(rule_MaKhoa.getRegex(), maKhoa)) {
            throw new Exception("Loi Regex: MaKhoa '" + maKhoa + "' khong dung dinh dang (vi du: CNTT).");
        }

        // Check TenKhoa
        if (rule_TenKhoa.isCheckNull() && (tenKhoa == null || tenKhoa.isEmpty())) throw new Exception("Loi: TenKhoa bi rong (NULL).");
        if (tenKhoa.length() > rule_TenKhoa.getMaxLength()) throw new Exception("Loi: TenKhoa qua dai (Max: " + rule_TenKhoa.getMaxLength() + ").");
    }
    
    private static boolean kiemTraMaKhoaDaTonTai(Connection conn, String maKhoa) throws SQLException {
        String sql = "SELECT 1 FROM KHOA_TBL WHERE MaKhoa = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, maKhoa);
            try (ResultSet rs = ps.executeQuery()) { return rs.next(); }
        }
    }
    
    private static void capNhatTrangThaiStaging(Connection conn, int stagingId, String status) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_UPDATE_STAGING)) {
            ps.setString(1, status);
            ps.setInt(2, stagingId);
            ps.executeUpdate();
        }
    }
    
    private static void ghiNhatKyLoi(Connection conn, int stagingId, String ruleName, String errorMessage) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_LOG_ERROR)) {
            ps.setObject(1, stagingId, Types.INTEGER); 
            ps.setString(2, ruleName);
            ps.setString(3, errorMessage);
            ps.executeUpdate();
        }
    }
}