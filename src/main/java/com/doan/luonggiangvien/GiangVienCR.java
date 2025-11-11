package com.doan.luonggiangvien; // Bro nên đổi tên package này cho đúng

import com.doan.util.ketnoi;
import com.doan.util.FieldRules;
import com.doan.util.GiangVienFieldRules; // Dùng POJO của bro
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
import java.util.regex.Pattern; // Import Regex

public class GiangVienCR {

    private static final String SQL_SELECT_STAGING = "SELECT * FROM STAGING_GIANGVIEN WHERE TrangThaiQC = 'PENDING'";
    private static final String SQL_INSERT_TARGET = "INSERT INTO GIANG_VIEN_TBL (MaGV, HoTen, MaKhoa, Email, SDT) VALUES (?, ?, ?, ?, ?)";
    private static final String SQL_UPDATE_STAGING = "UPDATE STAGING_GIANGVIEN SET TrangThaiQC = ? WHERE StagingID = ?";
    private static final String SQL_LOG_ERROR = "INSERT INTO QC_LOG_TBL (StagingID, RuleName, ErrorMessage) VALUES (?, ?, ?)";
    
    // Biến giữ "Sổ Quy định" (JSON)
    private static GiangVienFieldRules cauHinhRule; 

    public static void main(String[] args) {
        try {
            // Tải "Sổ Quy định"
            taiCauHinhRule();
            
            try (Connection conn = ketnoi.getConnectionDich()) { 
                conn.setAutoCommit(false); 
                try (Statement stmt = conn.createStatement();
                     ResultSet ketQuaStaging = stmt.executeQuery(SQL_SELECT_STAGING)) { // Đổi tên
                    
                    while (ketQuaStaging.next()) {
                        int stagingId = ketQuaStaging.getInt("StagingID");
                        try {
                            // Chạy "Dàn Bảo vệ"
                            kiemTraHopLe_GiangVien(conn, ketQuaStaging); 
                            
                            // --- XỬ LÝ KHI PASS ---
                            try (PreparedStatement psInsert = conn.prepareStatement(SQL_INSERT_TARGET)) {
                                psInsert.setString(1, ketQuaStaging.getString("MaGV"));
                                psInsert.setString(2, ketQuaStaging.getString("HoTen"));
                                psInsert.setString(3, ketQuaStaging.getString("MaKhoa"));
                                datChuoiChoPhepNull(psInsert, 4, ketQuaStaging.getString("Email"));
                                datChuoiChoPhepNull(psInsert, 5, ketQuaStaging.getString("SDT"));
                                psInsert.executeUpdate();
                            }
                            capNhatTrangThaiStaging(conn, stagingId, "PASSED");
                            System.out.println("PASSED [GiangVien]: StagingID " + stagingId);

                        } catch (Exception loi) { // Đổi tên
                            // --- XỬ LÝ KHI FAILED ---
                            System.err.println("FAILED [GiangVien]: StagingID " + stagingId + " - " + loi.getMessage());
                            ghiNhatKyLoi(conn, stagingId, "GiangVienCheckRule", loi.getMessage());
                            capNhatTrangThaiStaging(conn, stagingId, "FAILED");
                        }
                    }
                }
                conn.commit(); 
                System.out.println("Hoan tat kiem tra chat luong [GiangVien].");
            }
        } catch (Exception e) { e.printStackTrace(); }
    }
    
    // Đổi tên: loadRuleConfig
    private static void taiCauHinhRule() throws Exception {
        Gson gson = new Gson();
        InputStream is = GiangVienCR.class.getClassLoader().getResourceAsStream("giangvien_rules.json");
        if (is == null) throw new Exception("Loi: Khong tim thay file 'giangvien_rules.json'");
        Reader reader = new InputStreamReader(is);
        cauHinhRule = gson.fromJson(reader, GiangVienFieldRules.class); // Dùng POJO của bro
        System.out.println("Da load file config rule [GiangVien] thanh cong.");
    }
    
    // Đổi tên: validateGiangVien
    private static void kiemTraHopLe_GiangVien(Connection conn, ResultSet rs) throws Exception {
        FieldRules rule_MaGV = cauHinhRule.getMaGV();
        FieldRules rule_HoTen = cauHinhRule.getHoTen(); 
        FieldRules rule_MaKhoa = cauHinhRule.getMaKhoa();
        FieldRules rule_Email = cauHinhRule.getEmail();
        FieldRules rule_SDT = cauHinhRule.getSDT();
        
        String maGV = rs.getString("MaGV");
        String hoTen = rs.getString("HoTen"); 
        String maKhoa = rs.getString("MaKhoa");
        String email = rs.getString("Email");
        String sdt = rs.getString("SDT");
        
        // --- Bắt đầu chạy Check Rule ---
        
        // Check MaGV
        if (rule_MaGV.isCheckNull() && (maGV == null || maGV.isEmpty())) throw new Exception("Loi: MaGV bi rong (NULL).");
        if (rule_MaGV.isCheckDuplicate() && kiemTraMaGvDaTonTai(conn, maGV)) throw new Exception("Loi: MaGV '" + maGV + "' da ton tai.");
        if (rule_MaGV.getRegex() != null && !Pattern.matches(rule_MaGV.getRegex(), maGV)) {
            throw new Exception("Loi Regex: MaGV '" + maGV + "' khong dung dinh dang (vi du: GV001).");
        }
        
        // Check HoTen
        if (rule_HoTen.isCheckNull() && (hoTen == null || hoTen.isEmpty())) throw new Exception("Loi: HoTen bi rong (NULL).");
        if (rule_HoTen.getRegex() != null && !Pattern.matches(rule_HoTen.getRegex(), hoTen)) {
            throw new Exception("Loi Regex: HoTen '" + hoTen + "' co chua ky tu khong hop le.");
        }

        // Check MaKhoa
        if (rule_MaKhoa.isCheckNull() && (maKhoa == null || maKhoa.isEmpty())) throw new Exception("Loi: MaKhoa bi rong (NULL).");
        if (rule_MaKhoa.isCheckForeignKey() && !kiemTraMaKhoaDaTonTai(conn, maKhoa)) {
           throw new Exception("Loi Khoa Ngoai: MaKhoa '" + maKhoa + "' khong ton tai trong KHOA_TBL.");
        }
        
        // Check Email
        if (rule_Email.isCheckNull() && (email == null || email.isEmpty())) {
            throw new Exception("Loi: Email bi rong (NULL).");
        }
        if (rule_Email.getRegex() != null && !Pattern.matches(rule_Email.getRegex(), email)) {
            throw new Exception("Loi Regex: Email '" + email + "' khong dung dinh dang.");
        }

        // Check SDT
        if (rule_SDT.isCheckNull() && (sdt == null || sdt.isEmpty())) {
            throw new Exception("Loi: SDT bi rong (NULL).");
        }
        if (rule_SDT.getRegex() != null && !Pattern.matches(rule_SDT.getRegex(), sdt)) {
            throw new Exception("Loi Regex: SDT '" + sdt + "' khong dung dinh dang (10 so, bat dau bang 0).");
        }
    }
   
    private static boolean kiemTraMaGvDaTonTai(Connection conn, String maGV) throws SQLException {
        String sql = "SELECT 1 FROM GIANG_VIEN_TBL WHERE MaGV = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, maGV);
            try (ResultSet rs = ps.executeQuery()) { return rs.next(); }
        }
    }
    
    private static boolean kiemTraMaKhoaDaTonTai(Connection conn, String maKhoa) throws SQLException {
        // Hàm này check xem MaKhoa có tồn tại trong bảng cha KHOA_TBL không
        String sql = "SELECT 1 FROM KHOA_TBL WHERE MaKhoa = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, maKhoa);
             try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
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
    
    private static void datChuoiChoPhepNull(PreparedStatement ps, int index, String value) throws SQLException {
        if (value != null && !value.isEmpty()) ps.setString(index, value);
        else ps.setNull(index, Types.VARCHAR);
    }
}