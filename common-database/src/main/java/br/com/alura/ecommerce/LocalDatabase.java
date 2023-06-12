package br.com.alura.ecommerce;

import java.sql.*;

public class LocalDatabase {

    private final Connection connection;

    public LocalDatabase(String name) throws SQLException {
        this.connection = DriverManager.getConnection("jdbc:sqlite:service-users/target/" + name + ".db");
    }

    public void createNotIfExists(String sql) {
        try {
            this.connection.createStatement().execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public boolean update(String statement, String... params) throws SQLException {
        return preparedStatement(statement, params).execute();
    }

    public ResultSet query(String query, String... params) throws SQLException {
        return preparedStatement(query, params).executeQuery();
    }

    private PreparedStatement preparedStatement(String query, String[] params) throws SQLException {
        PreparedStatement preparedStatement = this.connection.prepareStatement(query);
        for (int i = 0; i < params.length; i++) {
            preparedStatement.setString(i + 1, params[i]);
        }
        return preparedStatement;
    }

    public void close() throws SQLException {
        connection.close();
    }
}
