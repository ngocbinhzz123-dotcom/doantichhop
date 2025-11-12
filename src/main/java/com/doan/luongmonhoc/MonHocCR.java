package com.doan.luongmonhoc;

import com.doan.util.ketnoi;
import com.doan.util.FieldRules;
import com.doan.util.MonHocFieldRules;
import com.google.gson.Gson;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.regex.Pattern;

public class MonHocCR { 

    private static final String SQL_SELECT_STAGING = "SELECT * FROM STAGING_MONHOC WHERE TrangThaiQC = 'PENDING'";
    private static final String SQL_INSERT_TARGET = "INSERT INTO MON_HOC_TBL (MaMon, TenMon, SoTinChi, HeSo) VALUES (?, ?, ?, ?)";
    private static final String SQL_UPDATE_STAGING = "UPDATE STAGING_MONHOC SET TrangThaiQC = ? WHERE StagingID = ?";
    private static final String SQL_LOG_ERROR = "INSERT INTO QC_LOG_TBL (StagingID, RuleName, ErrorMessage) VALUES (?, ?, ?)";
    
    private static MonHocFieldRules cauHinhRule;
    private static int soTinChi_DaCheck;
    private static double heSo_DaCheck; 

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
                            kiemTraHopLe_MonHoc(conn, ketQuaStaging); 
                            
                            try (PreparedStatement psInsert = conn.prepareStatement(SQL_INSERT_TARGET)) {
                                psInsert.setString(1, ketQuaStaging.getString("MaMon"));
                                psInsert.setString(2, ketQuaStaging.getString("TenMon"));
                                psInsert.setInt(3, soTinChi_DaCheck); 
                                psInsert.setDouble(4, heSo_DaCheck); 
                                psInsert.executeUpdate();
                            }
                            capNhatTrangThaiStaging(conn, stagingId, "PASSED");
                            System.out.println("PASSED [MonHoc]: StagingID " + stagingId);

                        } catch (Exception loi) { 
                            System.err.println("FAILED [MonHoc]: StagingID " + stagingId + " - " + loi.getMessage());
                            ghiNhatKyLoi(conn, stagingId, "MonHocCheckRule", loi.getMessage());
                            capNhatTrangThaiStaging(conn, stagingId, "FAILED");
                        }
                    }
                }
                conn.commit(); 
                System.out.println("Hoan tat kiem tra chat luong [MonHoc].");
            }
        } catch (Exception e) { e.printStackTrace(); }
    }
    
    private static void taiCauHinhRule() throws Exception {
        Gson gson = new Gson();
        InputStream is = MonHocCR.class.getClassLoader().getResourceAsStream("monhoc_rules.json");
        if (is == null) throw new Exception("Loi: Khong tim thay file 'monhoc_rules.json'");
        Reader reader = new InputStreamReader(is, StandardCharsets.UTF_8);
        cauHinhRule = gson.fromJson(reader, MonHocFieldRules.class); 
        System.out.println("Da load file config rule [MonHoc] thanh cong.");
    }
    
    private static void kiemTraHopLe_MonHoc(Connection conn, ResultSet rs) throws Exception {
        FieldRules rule_MaMon = cauHinhRule.getMaMon();
        FieldRules rule_TenMon = cauHinhRule.getTenMon();
        FieldRules rule_SoTinChi = cauHinhRule.getSoTinChi();
        FieldRules rule_HeSo = cauHinhRule.getHeSo();
        
        String maMon = rs.getString("MaMon");
        String tenMon = rs.getString("TenMon");
        String soTinChiStr = rs.getString("SoTinChi");
        String heSoStr = rs.getString("HeSo");
        
        // Check MaMon
        if (rule_MaMon.isCheckNull() && (maMon == null || maMon.isEmpty())) throw new Exception("Loi: MaMon bi rong (NULL).");
        if (rule_MaMon.isCheckDuplicate() && kiemTraMaMonDaTonTai(conn, maMon)) throw new Exception("Loi: MaMon '" + maMon + "' da ton tai.");
        if (rule_MaMon.getRegex() != null && !Pattern.matches(rule_MaMon.getRegex(), maMon)) throw new Exception("Loi Regex: MaMon '" + maMon + "' khong dung dinh dang (vi du: MH001).");
        
        // Check TenMon
        if (rule_TenMon.isCheckNull() && (tenMon == null || tenMon.isEmpty())) throw new Exception("Loi: TenMon bi rong (NULL).");

        // Check SoTinChi
        if (rule_SoTinChi.isCheckNull() && (soTinChiStr == null || soTinChiStr.isEmpty())) throw new Exception("Loi: SoTinChi bi rong (NULL).");
        if (rule_SoTinChi.getRegex() != null && !Pattern.matches(rule_SoTinChi.getRegex(), soTinChiStr)) throw new Exception("Loi Regex: SoTinChi '" + soTinChiStr + "' khong phai so nguyen duong hop le.");
        soTinChi_DaCheck = Integer.parseInt(soTinChiStr);
        
        // Check HeSo
        if (rule_HeSo.isCheckNull() && (heSoStr == null || heSoStr.isEmpty())) throw new Exception("Loi: HeSo bi rong (NULL).");
        try {
        	heSo_DaCheck = Double.parseDouble(heSoStr);
            if (heSo_DaCheck < rule_HeSo.getMin()) throw new Exception("Loi: HeSo phai >= " + rule_HeSo.getMin());
        } catch (Exception e) {
            throw new Exception("Loi: HeSo '" + heSoStr + "' khong phai so thap phan.");
        }
    }
    
    private static boolean kiemTraMaMonDaTonTai(Connection conn, String maMon) throws SQLException {
        String sql = "SELECT 1 FROM MON_HOC_TBL WHERE MaMon = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, maMon);
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