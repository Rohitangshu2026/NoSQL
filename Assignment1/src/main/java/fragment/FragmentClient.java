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
     * Initializes the JDBC connection to the baseline PostgreSQL database.
     *
     * Parameters:
     *   None
     *
     * Returns:
     *   None
     *
     * Behaviour:
     *   - Establishes a JDBC connection to the baseline PostgreSQL database
     *   - Authenticates using the local PostgreSQL user credentials
     *   - Stores the connection in the connection pool at index 0
     *   - Intended for generating expected_output.txt where all queries execute on a single database
     *
     * Errors:
     *   - Terminates execution if the database connection cannot be established
     */
    public void setupConnections() {
        Connection conn = null;
        try{
            String url = "jdbc:postgresql://localhost:5432/baseline";
            String user = "rohit2026";
            String password = "";

            conn = DriverManager.getConnection(url, user, password);
            connectionPool.put(0,conn);

            System.out.println("Connection established to baseline database");
        } catch(SQLException e){
            throw new RuntimeException("Connection to baseline database failed");
        }
    }

    /**
     * insertStudent
     * -------------
     * Inserts a student record into the baseline database.
     *
     * Parameters:
     *   studentId - unique student identifier
     *   name      - student name
     *   age       - student age
     *   email     - student email
     *
     * Behaviour:
     *   - Executes an INSERT on the Student table
     *   - Operates only on the baseline database (fragment 0)
     */
    public void insertStudent(String studentId, String name, int age, String email) {
        String sql = "INSERT INTO Student (student_id, name, age, email) VALUES (?, ?, ?, ?)";

        try(PreparedStatement stmt = connectionPool.get(0).prepareStatement(sql)){
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

        try(PreparedStatement stmt =  connectionPool.get(0).prepareStatement(sql)){
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

        try(PreparedStatement stmt =  connectionPool.get(0).prepareStatement(sql)){
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
        try(PreparedStatement stmt = connectionPool.get(0).prepareStatement(sql)){
            stmt.setString(1, studentId);
            stmt.setString(2, courseId);
            stmt.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * getStudentProfile
     * -----------------
     * Fetches basic profile information for a given student.
     *
     * Parameters:
     *   studentId - unique student identifier
     *
     * Returns:
     *   A comma-separated string of the form:
     *      "name,email"
     *   or NULL if the student does not exist.
     *
     * Behaviour:
     *   - Queries the Student table
     *   - Operates only on the baseline database (fragment 0)
     */
    public String getStudentProfile(String studentId) {
        String sql = "SELECT name, email FROM Student WHERE student_id = ?";

        try(PreparedStatement stmt = connectionPool.get(0).prepareStatement(sql)){
            stmt.setString(1, studentId);

            ResultSet rs = stmt.executeQuery();
            if(!rs.next())
                return null;
            return rs.getString("name") + "," + rs.getString("email");
        } catch (Exception e) {
            e.printStackTrace();
            return "ERROR";
        }
    }

    /**
     * getAvgScoreByDept
     * ----------------
     * Computes the average score for each department.
     *
     * Returns:
     *   A semicolon-separated string of the form:
     *     department,avg_score;department,avg_score
     *   or NULL if no grades exist.
     *
     * Behaviour:
     *   - Joins Grade and Course tables on course_id
     *   - Aggregates scores using AVG()
     *   - Operates only on the baseline database (fragment 0)
     */
    public String getAvgScoreByDept() {
        String sql = "SELECT Course.department, AVG(Grade.score) as avg_score FROM  " +
                "Grade JOIN Course ON Grade.course_id = Course.course_id " +
                "GROUP BY Course.department ORDER BY Course.department";

        StringBuilder result = new StringBuilder();

        try(PreparedStatement stmt = connectionPool.get(0).prepareStatement(sql)){
            ResultSet rs = stmt.executeQuery();

            boolean hasRows = false;
            while(rs.next()) {
                hasRows = true;
                String department = rs.getString("department");
                double avgScore = rs.getDouble("avg_score");
                result.append(String.format("%s:%.1f;", department, avgScore));
            }

            if(!hasRows)
                return null;

            // remove last semicolon
            result.deleteCharAt(result.length() - 1);
            return result.toString();

        } catch (Exception e) {
            e.printStackTrace();
            return "ERROR";
        }
    }

    /**
     * getAllStudentsWithMostCourses
     * -----------------------------
     * Finds all students who have enrolled in the maximum number of courses.
     *
     * Behaviour:
     *   - Counts number of courses per student
     *   - Finds the maximum course count
     *   - Returns all students matching that count
     *
     * Output format:
     *   student_id,name;student_id,name
     */
    public String getAllStudentsWithMostCourses() {
        String sql = "SELECT Student.student_id, Student.name " +
                "FROM Student " +
                "JOIN Grade ON Student.student_id = Grade.student_id " +
                "GROUP BY Student.student_id, Student.name " +
                "HAVING COUNT(Grade.course_id) = ( " +
                "  SELECT MAX(course_count) FROM ( " +
                "    SELECT COUNT(course_id) AS course_count " +
                "    FROM grade GROUP BY student_id " +
                "  ) sub " +
                ") " +
                "ORDER BY Student.student_id";

        StringBuilder result = new StringBuilder();
        try(PreparedStatement stmt = connectionPool.get(0).prepareStatement(sql)){
            ResultSet rs = stmt.executeQuery();

            boolean hasRows = false;

            while(rs.next()){
                hasRows = true;
                String studentId = rs.getString("student_id");
                String name = rs.getString("name");
                result.append(studentId + "," + name + ";");
            }

            if(!hasRows)
                return null;

            // remove last semicolon
            result.deleteCharAt(result.length() - 1);
            return result.toString();

        } catch (Exception e) {
            e.printStackTrace();
            return "ERROR";
        }
    }

    public void closeConnections() {
        for (Connection conn : connectionPool.values()) {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                    System.out.println("Closed connection to baseline database");
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
