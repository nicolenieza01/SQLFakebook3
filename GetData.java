import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TreeSet;
import java.util.Vector;

import org.json.JSONObject;
import org.json.JSONArray;

public class GetData {

    static String prefix = "project3.";

    // You must use the following variable as the JDBC connection
    Connection oracleConnection = null;

    // You must refer to the following variables for the corresponding 
    // tables in your database
    String userTableName = null;
    String friendsTableName = null;
    String cityTableName = null;
    String currentCityTableName = null;
    String hometownCityTableName = null;

    // DO NOT modify this constructor
    public GetData(String u, Connection c) {
        super();
        String dataType = u;
        oracleConnection = c;
        userTableName = prefix + dataType + "_USERS";
        friendsTableName = prefix + dataType + "_FRIENDS";
        cityTableName = prefix + dataType + "_CITIES";
        currentCityTableName = prefix + dataType + "_USER_CURRENT_CITIES";
        hometownCityTableName = prefix + dataType + "_USER_HOMETOWN_CITIES";
    }

    // TODO: Implement this function
    @SuppressWarnings("unchecked")
    public JSONArray toJSON() throws SQLException {

        // This is the data structure to store all users' information
        JSONArray users_info = new JSONArray();
        
        try (Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
            // Your implementation goes here....
            ResultSet rst = stmt.executeQuery(
                            "SELECT user_id, first_name, last_name, gender, year_of_birth, month_of_birth, day_of_birth " + 
                            "FROM " + userTableName); 

            while (rst.next()) { // step through result rows/records one by one
                int user_id = rst.getInt(1);

                try (Statement stmt2 = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
                    ResultSet rst2 = stmt2.executeQuery(
                            "SELECT user2_id " + 
                            "FROM " + friendsTableName + " " + 
                            "WHERE user1_id = " + user_id);
                    JSONArray friends = new JSONArray();
                    while (rst2.next()) { 
                        int friend_id = rst2.getInt(1);

                        friends.put(friend_id);
                    }

                    rst2 = stmt2.executeQuery(
                            "SELECT current_city_id " + 
                            "FROM " + currentCityTableName + " " + 
                            "WHERE user_id = " + user_id);
                    int current_id = -1;
                    while (rst2.next()) { 
                        current_id = rst2.getInt(1);
                    }

                    rst2 = stmt2.executeQuery(
                            "SELECT city_name, state_name, country_name " + 
                            "FROM " + cityTableName + " " + 
                            "WHERE city_id = " + current_id);
                    JSONObject current = new JSONObject();
                    while (rst2.next()) { 
                        current.put("city", rst2.getString(1));
                        current.put("state", rst2.getString(2));
                        current.put("country", rst2.getString(3));
                    }

                    rst2 = stmt2.executeQuery(
                            "SELECT hometown_city_id " + 
                            "FROM " + hometownCityTableName + " " + 
                            "WHERE user_id = " + user_id);
                    int hometown_id = -1;
                    while (rst2.next()) { 
                        hometown_id = rst2.getInt(1);
                    }

                    rst2 = stmt2.executeQuery(
                            "SELECT city_name, state_name, country_name " + 
                            "FROM " + cityTableName + " " + 
                            "WHERE city_id = " + hometown_id);
                    JSONObject hometown = new JSONObject();
                    while (rst2.next()) { 
                        hometown.put("city", rst2.getString(1));
                        hometown.put("state", rst2.getString(2));
                        hometown.put("country", rst2.getString(3));
                    }
                    JSONObject user_to_add = new JSONObject();
                    user_to_add.put("user_id", user_id);
                    user_to_add.put("first_name", rst.getString(2));
                    user_to_add.put("last_name", rst.getString(3));
                    user_to_add.put("gender", rst.getString(4));
                    user_to_add.put("YOB", rst.getLong(5));
                    user_to_add.put("MOB", rst.getLong(6));
                    user_to_add.put("DOB", rst.getLong(7));
                    user_to_add.put("friends", friends);
                    user_to_add.put("hometown", hometown);
                    user_to_add.put("current", current);
                    users_info.put(user_to_add);
                    stmt2.close();
                } catch (SQLException e) {
                    System.err.println(e.getMessage());
                }
                
            }
            
            stmt.close();
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return users_info;
    }

    // This outputs to a file "output.json"
    // DO NOT MODIFY this function
    public void writeJSON(JSONArray users_info) {
        try {
            FileWriter file = new FileWriter(System.getProperty("user.dir") + "/output.json");
            file.write(users_info.toString());
            file.flush();
            file.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
