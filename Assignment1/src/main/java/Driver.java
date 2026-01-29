import java.io.*;
import java.util.Scanner;
import fragment.FragmentClient;

public class Driver {
    private static final int NUM_FRAGMENTS = 3;

    public static void main(String[] args) {
        FragmentClient client = new FragmentClient(NUM_FRAGMENTS);

        try {
            System.out.println("Initializing connections...");
            client.setupConnections();

            InputStream in = Driver.class
                    .getClassLoader()
                    .getResourceAsStream("workload.txt");

            if (in == null) {
                System.err.println("Error: workload.txt not found on classpath.");
                return;
            }
            Scanner scanner = new Scanner(in);
            PrintWriter outputWriter = new PrintWriter("output.txt");

            System.out.println("Processing workload...");
            long startTime = System.currentTimeMillis();

            while (scanner.hasNextLine()) {
                String line = scanner.nextLine().trim();
                if (line.isEmpty()) continue;

                String[] parts = line.split(",");
                String command = parts[0];

                try {
                    switch (command) {
                        case "INSERT_STUDENT":
                            client.insertStudent(
                                    parts[1], parts[2],
                                    Integer.parseInt(parts[3]), parts[4]);
                            break;

                        case "INSERT_GRADE":
                            client.insertGrade(
                                    parts[1], parts[2],
                                    Integer.parseInt(parts[3]));
                            break;

                        case "UPDATE_GRADE":
                            client.updateGrade(
                                    parts[1], parts[2],
                                    Integer.parseInt(parts[3]));
                            break;

                        case "DELETE_STUDENT_COURSE":
                            client.deleteStudentFromCourse(parts[1], parts[2]);
                            break;

                        case "READ_PROFILE":
                            String profile = client.getStudentProfile(parts[1]);
                            outputWriter.println(profile != null ? profile : "NULL");
                            break;

                        case "READ_SCORE":
                            String gpa = client.getAvgScoreByDept();
                            outputWriter.println(gpa != null ? gpa : "NULL");
                            break;

                        case "READ_ALL":
                            String top = client.getAllStudentsWithMostCourses();
                            outputWriter.println(top != null ? top : "NULL");
                            break;

                        default:
                            outputWriter.println("ERROR: Unknown command " + command);
                    }
                } catch (Exception e) {
                    outputWriter.println("ERROR: " + e.getMessage());
                }
            }

            long endTime = System.currentTimeMillis();
            System.out.println("Workload finished in " + (endTime - startTime) + "ms");

            scanner.close();
            outputWriter.close();
            client.closeConnections();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

