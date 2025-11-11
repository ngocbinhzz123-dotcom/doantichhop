package com.doan.luongsinhvien;

import com.doan.util.ketnoi;
import com.doan.util.FieldRules; 
import com.doan.util.SinhVienFieldRules;
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

public class SinhVienCR {

    private static final String SQL_SELECT_STAGING = "SELECT * FROM STAGING_SINHVIEN WHERE TrangThaiQC = 'PENDING'";
    private static final String SQL_INSERT_TARGET = "INSERT INTO SINH_VIEN_TBL (MaSV, HoTen, NgaySinh, GioiTinh, MaLop, MaKhoa, Email, SDT) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String SQL_UPDATE_STAGING = "UPDATE STAGING_SINHVIEN SET TrangThaiQC = ? WHERE StagingID = ?";
    private static final String SQL_LOG_ERROR = "INSERT INTO QC_LOG_TBL (StagingID, RuleName, ErrorMessage) VALUES (?, ?, ?)";
    
    private static SinhVienFieldRules cauHinhRule;
    
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
                            kiemTraHopLe_SinhVien(conn, ketQuaStaging); 
                            
                            try (PreparedStatement psInsert = conn.prepareStatement(SQL_INSERT_TARGET)) {
                                psInsert.setString(1, ketQuaStaging.getString("MaSV"));
                                psInsert.setString(2, ketQuaStaging.getString("HoTen"));
                                psInsert.setDate(3, java.sql.Date.valueOf(ketQuaStaging.getString("NgaySinh"))); 
                                psInsert.setString(4, ketQuaStaging.getString("GioiTinh"));
                                psInsert.setString(5, ketQuaStaging.getString("MaLop"));
                                psInsert.setString(6, ketQuaStaging.getString("MaKhoa"));
                                datChuoiChoPhepNull(psInsert, 7, ketQuaStaging.getString("Email"));
                                datChuoiChoPhepNull(psInsert, 8, ketQuaStaging.getString("SDT"));
                                psInsert.executeUpdate();
                            }
                            capNhatTrangThaiStaging(conn, stagingId, "PASSED");
                            System.out.println("PASSED [SinhVien]: StagingID " + stagingId);

                        } catch (Exception loi) {
                            System.err.println("FAILED [SinhVien]: StagingID " + stagingId + " - " + loi.getMessage());
                            ghiNhatKyLoi(conn, stagingId, "SinhVienCheckRule", loi.getMessage());
                            capNhatTrangThaiStaging(conn, stagingId, "FAILED");
                        }
                    }
                }
                conn.commit(); 
                System.out.println("Hoan tat kiem tra chat luong [SinhVien].");
            }
        } catch (Exception e) { e.printStackTrace(); }
    }
    
    private static void taiCauHinhRule() throws Exception {
        Gson gson = new Gson();
        InputStream is = SinhVienCR.class.getClassLoader().getResourceAsStream("sinhvien_rules.json");
        if (is == null) throw new Exception("Loi: Khong tim thay file 'sinhvien_rules.json'");
        Reader reader = new InputStreamReader(is);
        cauHinhRule = gson.fromJson(reader, SinhVienFieldRules.class);
        System.out.println("Da load file config rule [SinhVien] thanh cong.");
    }
    
    private static void kiemTraHopLe_SinhVien(Connection conn, ResultSet rs) throws Exception {
        FieldRules rule_MaSV = cauHinhRule.getMaSV();
        FieldRules rule_HoTen = cauHinhRule.getHoTen();
        FieldRules rule_NgaySinh = cauHinhRule.getNgaySinh();
        FieldRules rule_MaKhoa = cauHinhRule.getMaKhoa(); 
        FieldRules rule_Email = cauHinhRule.getEmail();
        FieldRules rule_SDT = cauHinhRule.getSDT();
        
        String maSV = rs.getString("MaSV");
        String hoTen = rs.getString("HoTen");
        String ngaySinhStr = rs.getString("NgaySinh");
        String maKhoa = rs.getString("MaKhoa");
        String email = rs.getString("Email");
        String sdt = rs.getString("SDT");
        
        // Check MaSV
        if (rule_MaSV.isCheckNull() && (maSV == null || maSV.isEmpty())) throw new Exception("Loi: MaSV bi rong (NULL).");
        if (rule_MaSV.getRegex() != null && !Pattern.matches(rule_MaSV.getRegex(), maSV)) throw new Exception("Loi Regex: MaSV '" + maSV + "' khong dung dinh dang (vi du: SV001).");
        if (rule_MaSV.isCheckDuplicate() && kiemTraMaSvDaTonTai(conn, maSV)) throw new Exception("Loi: MaSV '" + maSV + "' da ton tai.");
        
        // Check HoTen
        if (rule_HoTen.isCheckNull() && (hoTen == null || hoTen.isEmpty())) throw new Exception("Loi: HoTen bi rong (NULL).");
        if (hoTen.length() > rule_HoTen.getMaxLength()) throw new Exception("Loi: HoTen qua dai (Max: " + rule_HoTen.getMaxLength() + ").");
        if (rule_HoTen.getRegex() != null && !Pattern.matches(rule_HoTen.getRegex(), hoTen)) throw new Exception("Loi Regex: HoTen '" + hoTen + "' co chua ky tu khong hop le.");

        // Check NgaySinh
        if (rule_NgaySinh.isCheckNull() && (ngaySinhStr == null || ngaySinhStr.isEmpty())) throw new Exception("Loi: NgaySinh bi rong (NULL).");
        if (rule_NgaySinh.getRegex() != null && !Pattern.matches(rule_NgaySinh.getRegex(), ngaySinhStr)) throw new Exception("Loi Regex: NgaySinh '" + ngaySinhStr + "' khong dung dinh dang YYYY-MM-DD.");
        
        // Check MaKhoa
        if (rule_MaKhoa.isCheckNull() && (maKhoa == null || maKhoa.isEmpty())) throw new Exception("Loi: MaKhoa bi rong (NULL).");
        if (rule_MaKhoa.isCheckForeignKey() && !kiemTraMaKhoaDaTonTai(conn, maKhoa)) throw new Exception("Loi Khoa Ngoai: MaKhoa '" + maKhoa + "' khong ton tai trong bang KHOA_TBL.");
        
        // Check Email
        if (rule_Email.isCheckNull() && (email == null || email.isEmpty())) throw new Exception("Loi: Email bi rong (NULL).");
        if (rule_Email.getRegex() != null && !Pattern.matches(rule_Email.getRegex(), email)) throw new Exception("Loi Regex: Email '" + email + "' khong dung dinh dang.");

        // Check SDT
        if (rule_SDT.isCheckNull() && (sdt == null || sdt.isEmpty())) throw new Exception("Loi: SDT bi rong (NULL).");
        if (rule_SDT.getRegex() != null && !Pattern.matches(rule_SDT.getRegex(), sdt)) throw new Exception("Loi Regex: SDT '" + sdt + "' khong dung dinh dang (10 so, bat dau bang 0).");
    }
    
    private static boolean kiemTraMaSvDaTonTai(Connection conn, String maSV) throws SQLException {
        String sql = "SELECT 1 FROM SINH_VIEN_TBL WHERE MaSV = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, maSV);
            try (ResultSet rs = ps.executeQuery()) { return rs.next(); }
        }
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
    private static void datChuoiChoPhepNull(PreparedStatement ps, int index, String value) throws SQLException {
        if (value != null && !value.isEmpty()) ps.setString(index, value);
        else ps.setNull(index, Types.VARCHAR);
    }
}