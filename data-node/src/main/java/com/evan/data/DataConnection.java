package com.evan.data;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.evan.core.Config;

public class DataConnection {
    private final Config config;

    public DataConnection() {
        this.config = new Config().load_parameters();
    }

    public Connection connectAndInitialize() {
        Connection conn = null;

        String sql = "CREATE TABLE IF NOT EXISTS kv_store (" +
                "key TEXT PRIMARY KEY, " +
                "value DOUBLE);";

        try {
            // get path to this file's parent folder and create a data/ folder if it doesnt
            // exist
            String cur_dir = System.getProperty("user.dir");
            String data_dir = cur_dir + config.getDatabase().getPath();
            File data_folder = new File(data_dir);
            if (!data_folder.exists()) {
                data_folder.mkdirs();
            }

            // initialize the connection
            String url = "jdbc:sqlite:" + data_dir + config.getDatabase().getName();
            conn = DriverManager.getConnection(url);

            // create the table if it doesnt exist yet
            Statement stmt = conn.createStatement();
            stmt.execute(sql);

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return conn;
    }

    public boolean put(Connection conn, String key, double value) {
        String sql = "INSERT OR REPLACE INTO kv_store(key, value) VALUES (?, ?);";
        try {
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, key);
            pstmt.setDouble(2, value);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return false;
        }
        return true;
    }

    // return the value but the value is of type BLOB or like nothing to indicate
    // failure
    public Double get(Connection conn, String key) {
        String sql = "SELECT value FROM kv_store WHERE key = ?;";
        try {
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, key);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getDouble("value");
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return null;
        }
        return null;
    }

    public boolean delete(Connection conn, String key) {
        String sql = "DELETE FROM kv_store WHERE key = ?;";
        try {
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, key);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return false;
        }
        return true;
    }
}
