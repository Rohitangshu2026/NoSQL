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
     * insertGrade
     * -----------
     * Inserts a grade record into the baseline database.
     *
     * Parameters:
     *   studentId - unique student identifier
     *   courseId  - course identifier
     *   score     - numeric score obtained by the student
     *
     * Behaviour:
     *   - Executes an INSERT on the Grade table
     *   - Operates only on the baseline database (fragment 0)
     *
     * Errors:
     *   - Prints stack trace if insertion fails
     */
    public void insertStudent(String studentId, String name, int age, String email) {
        String sql = "INSERT INTO Student (student_id, name, age, email) VALUES (?, ?, ?, ?)";
        int shard = router.getFragmentId(studentId);

        try(PreparedStatement stmt = connectionPool.get(shard).prepareStatement(sql)){
            stmt.setString(1, studentId);
            stmt.setString(2, name);
            stmt.setInt(3, age);
            stmt.setString(4, email);
            stmt.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * insertGrade
     * -----------
     * Inserts a grade record into the baseline database.
     *
     * Parameters:
     *   studentId - unique student identifier
     *   courseId  - course identifier
     *   score     - numeric score obtained by the student
     *
     * Behaviour:
     *   - Executes an INSERT on the Grade table
     *   - Operates only on the baseline database (fragment 0)
     *
     * Errors:
     *   - Prints stack trace if insertion fails
     */
    public void insertGrade(String studentId, String courseId, int score) {
        String sql = "INSERT INTO Grade (student_id, course_id, score) VALUES (?, ?, ?)" +
                "ON CONFLICT (student_id, course_id) DO NOTHING";
        int shard = router.getFragmentId(studentId);

        try(PreparedStatement stmt =  connectionPool.get(shard).prepareStatement(sql)){
            stmt.setString(1, studentId);
            stmt.setString(2, courseId);
            stmt.setInt(3, score);
            stmt.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * updateGrade
     * -----------
     * Updates the score for an existing student-course entry
     * in the baseline database.
     *
     * Parameters:
     *   studentId - student identifier
     *   courseId  - course identifier
     *   newScore  - updated score
     *
     * Behaviour:
     *   - Executes an UPDATE on the Grade table
     *   - Operates only on the baseline database (fragment 0)
     * Errors:
     *   - Prints stack trace if insertion fails
     */
    public void updateGrade(String studentId, String courseId, int newScore) {
        String sql = "UPDATE Grade SET score = ? WHERE student_id = ? AND course_id = ?";
        int shard = router.getFragmentId(studentId);

        try(PreparedStatement stmt =  connectionPool.get(shard).prepareStatement(sql)){
            stmt.setInt(1, newScore);
            stmt.setString(2, studentId);
            stmt.setString(3, courseId);
            stmt.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * deleteStudentFromCourse
     * -----------------------
     * Deletes a specific course enrollment for a student
     * from the baseline database.
     *
     * Parameters:
     *   studentId - unique student identifier
     *   courseId  - course identifier
     *
     * Behaviour:
     *   - Executes a DELETE on the Grade table
     *   - Removes only the specified (student_id, course_id) pair
     *   - Does not delete the student record itself
     *   - Operates only on the baseline database (fragment 0)
     */
    public void deleteStudentFromCourse(String studentId, String courseId) {
        String sql = "DELETE FROM Grade WHERE student_id = ? AND course_id = ?";
        int shard = router.getFragmentId(studentId);

        try(PreparedStatement stmt = connectionPool.get(shard).prepareStatement(sql)){
            stmt.setString(1, studentId);
            stmt.setString(2, courseId);
            stmt.executeUpdate();
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
