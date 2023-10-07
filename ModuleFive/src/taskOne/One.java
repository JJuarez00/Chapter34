/*
Chapter 34 - 34.11


SQL code to delete everything from table: DELETE FROM Babyname;

*/

package taskOne;

import java.sql.*;
import java.io.*;
import java.net.URI; // Import URI instead of URL
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class One {

    public static void main(String[] args) throws URISyntaxException {
        Connection connection = null;
        String sql = "INSERT INTO Babyname (year, name, gender, count) VALUES (?, ?, ?, ?)";
        
        try {
            // Database Connection
            String username = "Joseph";
            String password = "12004270";
            String url = "jdbc:mysql://localhost:3306/Baby";
            connection = DriverManager.getConnection(url, username, password);

            // Process 2001 data
            importDataFromURI(connection, sql, new URI("https://liveexample.pearsoncmg.com/data/babynamesranking2001.txt"), "2001");

            // Process 2010 data
            importDataFromURI(connection, sql, new URI("https://liveexample.pearsoncmg.com/data/babynamesranking2010.txt"), "2010");

        } catch (SQLException e) {
            System.out.println("Database connection error: " + e.getMessage());
        } finally {
            try {
                if (connection != null)
                    connection.close();
            } catch (SQLException ex) {
                System.out.println("Error closing connection: " + ex.getMessage());
            }
        }
    }

    private static void importDataFromURI(Connection connection, String sql, URI uri, String year) {
        try {
            InputStream inputStream = uri.toURL().openStream(); // Convert URI to URL
            try (
                BufferedReader in = new BufferedReader(new InputStreamReader(inputStream))) {
                String inputLine;
                int count = 0;
                List<String> dataList = new ArrayList<>();
                while ((inputLine = in.readLine()) != null) {
                    dataList.clear(); // Clear the list for each line
                    String[] data = inputLine.split("\\s+");
                    
                    
                    dataList.addAll(Arrays.asList(data));
                    
                    if (dataList.size() >= 4) {
                        count++;
                        String name1 = dataList.get(1);
                        String name2 = dataList.get(3);
                        insertData(connection, sql, year, name1, "Male", count);
                        insertData(connection, sql, year, name2, "Female", count);
                    }
                }
            }
        } catch (IOException e) {
            System.out.println("Error fetching data from URL: " + e.getMessage());
        }
    }

    private static void insertData(Connection connection, String sql, String year, String name, String gender, int count) {
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, year);
            preparedStatement.setString(2, name);
            preparedStatement.setString(3, gender);
            preparedStatement.setInt(4, count);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            System.out.println("Error inserting data: " + e.getMessage());
        }
    }
}
