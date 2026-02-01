package fragment;
import java.sql.*;
import java.util.*;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
public class FragmentClient {

    private Map<Integer, Connection> connectionPool;
    private Router router;
    private int numFragments;

    public FragmentClient(int numFragments) {
        this.numFragments = numFragments;
        this.router = new Router(numFragments);
        this.connectionPool = new HashMap<>();
    }

    /**
     * setupConnections
     * ----------------
     * Initializes JDBC connections to all database fragments.
     *
     * Behaviour:
     *   - Creates one JDBC connection per fragment
     *   - Stores each connection in connectionPool using fragment as key
     *   - Databases are named fragment0, fragment1, ..., fragmentN-1
     *
     * Errors:
     *   - Terminates execution if any fragment connection fails
     */
    public void setupConnections() {
		try{
            String host = "localhost";
            String port = "5432";
            String user = "rohit2026";
            String password = "";

            for(int fragment = 0; fragment < numFragments; ++fragment){
                String databaseName = "fragment" + fragment;
                String url = "jdbc:postgresql://" + host + ":" + port + "/" + databaseName;
                Connection connection = DriverManager.getConnection(url,user,password);
                connectionPool.put(fragment, connection);

                System.out.println("Connection established to fragment: " + fragment);
            }
        }
        catch(SQLException e){
            throw new RuntimeException("Failed to connect to all fragments", e);
        }
    }

    /**
     * TODO: Route the student to the correct shard and execute the INSERT.
     */
    public void insertStudent(String studentId, String name, int age, String email) {
        try {
            // Your code here:
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * TODO: Route the grade to the correct shard and execute the INSERT.
     */
    public void insertGrade(String studentId, String courseId, int score) {
        try {
            // Your code here
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void updateGrade(String studentId, String courseId, int newScore) {
        try {
		// Your code here:
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void deleteStudentFromCourse(String studentId, String courseId) {
        try {
	// Your code here:
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * TODO: Fetch the student's name and email.
     */
    public String getStudentProfile(String studentId) {
        try {
            // Your code here
            return null; 
            
        } catch (Exception e) {
            e.printStackTrace();
            return "ERROR";
        }
    }

    /**
     * TODO: Calculate the average score per department.
     */
    public String getAvgScoreByDept() {
        try {
            // Your code here
            return null;

        } catch (Exception e) {
            e.printStackTrace();
            return "ERROR";
        }
    }

    /**
     * TODO: Find all the students that have taken most number of courses
     */
    public String getAllStudentsWithMostCourses() {
        try {
            // Your code here
            return null;

        } catch (Exception e) {
            e.printStackTrace();
            return "ERROR";
        }
    }

    public void closeConnections() {
        
    }
}
