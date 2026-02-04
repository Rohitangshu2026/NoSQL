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
     * getAvgScoreByDept
     * -----------------
     * Computes the average score for each department across all database fragments.
     *
     * Behaviour:
     *   - Executes a fragment-local aggregation on each fragment to compute:
     *       (department, SUM(score), COUNT(*))
     *   - Merges partial aggregates in the coordinator (Java) to obtain
     *     global totals per department
     *   - Computes the final average as:
     *       totalScore / totalCount
     *   - Returns results sorted by department name
     *
     * Output Format:
     *   "CS:75.5;Math:82.3;Physics:78.0"
     *
     * Notes:
     *   - Performs a global aggregation without cross-fragment joins
     *   - Parallel-safe due to student_id-based horizontal partitioning
     *
     * Returns:
     *   - A formatted string containing average scores per department
     *   - null if no grade records exist
     *   - "ERROR" if an exception occurs
     */
    public String getAvgScoreByDept() {
        Map<String, Integer> totalScore = new HashMap<>();
        Map<String, Integer> totalCount = new HashMap<>();

        String sql = "SELECT course.department, SUM(score) as score, count(*) as cnt " +
                "FROM grade JOIN course ON grade.course_id = course.course_id " +
                "GROUP BY course.department";
        try {
            // aggregate from fragments
            for(int fragment = 0; fragment < numFragments; ++ fragment) {
                Connection conn = connectionPool.get(fragment);
                try(Statement statement = conn.createStatement();
                    ResultSet rs = statement.executeQuery(sql)){

                        while(rs.next()){
                            String department = rs.getString("department");
                            int sumScore = rs.getInt("score");
                            int count = rs.getInt("cnt");
                            totalScore.put(department, totalScore.getOrDefault(department, 0) + sumScore);
                            totalCount.put(department, totalCount.getOrDefault(department, 0) + count);
                        }
                }
            }

            if (totalScore.isEmpty())
                return null;

            // global aggregate
            List<String> departments =  new ArrayList<>(totalScore.keySet());
            Collections.sort(departments);
            StringBuilder result = new StringBuilder();
            for (String department : departments) {
                double avgScore = (double) totalScore.get(department) / totalCount.get(department);
                result.append(String.format("%s:%.1f;", department, avgScore));
            }

            // remove last semicolon
            result.setLength(result.length() - 1);
            return result.toString();

        } catch (Exception e) {
            e.printStackTrace();
            return "ERROR";
        }
    }

    /**
     * getAllStudentsWithMostCourses
     * -----------------------------
     * Finds all students who are enrolled in the maximum number of courses
     * across all database fragments.
     *
     * Behaviour:
     *   - Identifies students having the maximum course count
     *     within a fragment
     *   - Collects local results from each fragment
     *   - Computes the global maximum course count
     *   - Filters students whose course count equals the global maximum
     *   - Sorts the final result by student_id for deterministic output
     *
     * Output Format:
     *   "student_id,name;student_id,name"
     *
     * Notes:
     *   - Uses student_id-based horizontal partitioning
     *   - Each student exists in exactly one fragment
     *   - Avoids cross-fragment joins by using a two-phase aggregation strategy
     *
     * Returns:
     *   - A formatted string of students with the highest course enrollment
     *   - null if no enrollments exist
     *   - "ERROR" if an exception occurs
     */


    public String getAllStudentsWithMostCourses() {
        class StudentInfo{
            String studentId;
            String name;
            int courseCount;

            StudentInfo(String studentId, String name, int courseCount){
                this.studentId = studentId;
                this.name = name;
                this.courseCount = courseCount;
            }
        }

        List<StudentInfo> localMaxResult = new ArrayList<>();

        String sql = "SELECT s.student_id, s.name, COUNT(g.course_id) AS cnt " +
                "FROM student as s JOIN grade as g ON s.student_id = g.student_id " +
                "GROUP BY s.student_id,s.name " +
                "HAVING COUNT(g.course_id) = ( " +
                "   SELECT MAX(course_count) FROM ( " +
                "       SELECT COUNT(course_id) AS course_count " +
                "       FROM grade " +
                "       GROUP BY student_id " +
                "   ) AS course_counts " +
                "   )";
        try {
            // get students with max course count from each fragment
            for(int fragment = 0; fragment < numFragments; ++fragment){
                Connection conn = connectionPool.get(fragment);

                try(PreparedStatement statement = conn.prepareStatement(sql);
                    ResultSet rs = statement.executeQuery()){
                    while(rs.next()){
                        localMaxResult.add(new StudentInfo(rs.getString("student_id"), rs.getString("name"), rs.getInt("cnt")));
                    }
                }
            }

            if(localMaxResult.isEmpty())
                return null;

            // get students with max course count from all fragments
            int globalMax = 0;
            for(StudentInfo student : localMaxResult){
                globalMax = Math.max(globalMax, student.courseCount);
            }

            List<StudentInfo> globalMaxResult = new ArrayList<>();
            for(StudentInfo student: localMaxResult){
                if(student.courseCount == globalMax)
                    globalMaxResult.add(student);
            }

            // order the students for deterministic output
            globalMaxResult.sort(Comparator.comparing(a -> a.studentId));

            StringBuilder result = new StringBuilder();
            for (StudentInfo s : globalMaxResult) {
                result.append(s.studentId)
                        .append(",")
                        .append(s.name)
                        .append(";");
            }

            result.setLength(result.length() - 1);
            return result.toString();

        } catch (Exception e) {
            e.printStackTrace();
            return "ERROR";
        }
    }


    public void closeConnections() {

    }
}
