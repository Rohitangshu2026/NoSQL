# Assignment 1 
## SimuFragDB – Simulating a Distributed RDBMS

---

### Overview

**SimuFragDB** is a simulated distributed relational database system built on top of PostgreSQL.
It simulates a multi-node distributed database by running multiple PostgreSQL instances (fragments) on a single machine, each storing a horizontal partition of the data.

The system demonstrates:
- Horizontal fragmentation
- Deterministic query routing
- Fragment-local execution
- Application-level aggregation across fragments

This assignment focuses on understanding distributed database concepts, not on strict consistency or fault tolerance.

---

### Architecture

#### Databases
- baseline- Single PostgreSQL database used to generate `expected_output.txt`
- fragment0, fragment1, fragment2 – Horizontally partitioned shards

Each fragment is a separate PostgreSQL database
#### Partitioning Strategy
- Data is horizontally partitioned using a deterministic hash function:
  - fragmentId = abs(key.hashCode()) % numFragments
  - student_id is used as the routing key.
- Each student exists in exactly one fragment
- Enables fragment-local operations without cross-fragment joins
fragment0, fragment1, fragment2, …

Queries are explicitly routed to fragments via Router.java.

---


### PostgreSQL Database Setup


```bash
psql -U <your_username> postgres
```

Create the baseline database:

```sql
CREATE DATABASE baseline;
```

Create fragment databases:

```sql
CREATE DATABASE fragment0;
CREATE DATABASE fragment1;
CREATE DATABASE fragment2;
```

Exit PostgreSQL:
```sql
\q
```
---

### Database Schema
The following schema is created on every fragment using `scripts.sql`:
```SQL
CREATE TABLE Student (
student_id VARCHAR(50) PRIMARY KEY,
name VARCHAR(100),
age INT,
email VARCHAR(100)
);

CREATE TABLE Grade (
student_id VARCHAR(50),
course_id VARCHAR(20),
score INT,
PRIMARY KEY (student_id, course_id),
FOREIGN KEY (student_id) REFERENCES Student(student_id)
);

CREATE TABLE Course (
course_id VARCHAR(20) PRIMARY KEY,
course_name VARCHAR(100),
department VARCHAR(50)
);
```

The Course table is replicated on all fragments.

```SQL
INSERT INTO Course (course_id, course_name, department) VALUES
('CS101', 'Intro to NoSQL', 'CS'),
('CS102', 'Operating Systems', 'CS'),
('MA101', 'Calculus I', 'Math'),
('MA102', 'Linear Algebra', 'Math'),
('PH101', 'Physics I', 'Physics');
```

---

### Project Structure

```
Assignment1/
├── src/
│   ├── main/
│   │    └── java/
│   │         ├── Driver.java
│   │         └── fragment/
│   │             ├── FragmentClient.java
│   │             └── Router.java
│   └── resources
│        └── workload.txt   
├── output/
│   ├── output.txt
│   └── expected_output.txt
├── report.pdf
└── pom.xml
```
---
### Key Components
`Router`

- Determines fragment ID using `student_id`

- Ensures consistent routing for inserts, updates, and deletes

`FragmentClient`

 Handles all database operations:

- Connection setup for all fragments

- Routed inserts and updates

- Fragment-local aggregation

- Application-side merging of results

`Driver`

- Reads commands from `workload.txt`

- Executes operations sequentially

- Writes query results to `output.txt`

---
### Supported Operations
#### Write Operations

- `INSERT_STUDENT`

- `INSERT_GRADE`

- `UPDATE_GRADE`

- `DELETE_STUDENT_COURSE`

#### Read Operations

- `READ_PROFILE`

- `READ_SCORE` (average score by department)

- `READ_ALL` (students with maximum course enrollments)

---

### Implemented Functionalities
#### Connection Management

- `setupConnections()`: Establishes JDBC connections to all fragments and stores them in a connection pool.

- `closeConnections()`: Gracefully closes all fragment connections.

---

#### Data Modification Operations

- `insertStudent(studentId, name, age, email)`:
Routes the insert using studentId and inserts into the correct fragment.

- `insertGrade(studentId, courseId, score)`:
Inserts grade into the same fragment as the student.

- `updateGrade(studentId, courseId, newScore)`:
Updates an existing grade using deterministic routing.

- `deleteStudentFromCourse(studentId, courseId)`:
Deletes a specific (student_id, course_id) entry from the correct fragment.

---

#### Read Operations

- `getStudentProfile(studentId)`:
Fetches student name and email from the routed fragment.
 
   Output format:

        name,email

---

#### Global Aggregation Queries
**Average Score by Department**

- `getAvgScoreByDept()`:

**Approach:**

1. Each fragment computes:

        (department, SUM(score), COUNT(*))


2. The application code merges partial results.

3. Final averages are computed as:

       totalScore / totalCount


**Output format:**

    CS:75.5;Math:82.3;Physics:78.0


No cross-fragment joins are used.

---

#### Students with Maximum Course Enrollments

- `getAllStudentsWithMostCourses()`:

**Approach:**

1. Each fragment finds students with the maximum course count locally.

2. The application code determines the global maximum.

3. Students matching the global maximum are returned.

**Output format:**

    student_id,name;student_id,name


Results are sorted by `student_id` for deterministic output.

---

#### Workload Execution

- Input operations are read from workload.txt

- `Driver.java` executes operations sequentially

- Outputs are written to:

  - `output.txt` (distributed setting)

  - `expected_output.txt` (baseline setting)

---

### Baseline vs Distributed Validation

To verify correctness:

1. Run workload on a single PostgreSQL database (baseline) → expected_output.txt

2. Run workload on fragmented setup (fragment0, fragment1, fragment2)→ output.txt

3. Compare outputs line-by-line

The baseline and distributed outputs are formatted identically for direct comparison.

--- 

### Setup Instructions

1. Install:

   - Java 17

   - Maven

   - PostgreSQL

2. Create fragment databases:

       fragment0, fragment1, fragment2, ...


3. Run `scripts.sql` on each fragment

4. Build and run:

       mvn clean install
       mvn dependency:copy-dependencies
       java -cp "target/classes:target/dependency/*" Driver

5. Verify:
        
       diff output/output.txt output/expected_output.txt

---

### Contributors

**Rohitangshu Bose:**

- `setupConnections()`
- `getAvgScoreByDept()`
- `getAllStudentsWithMostCourses()`

**Shwetank Singh:** 

- `insertStudent(studentId, name, age, email)`
- `insertGrade(studentId, courseId, score)`
- `updateGrade(studentId, courseId, newScore)`

**Manzil Baruah:** 

- `deleteStudentFromCourse(studentId, courseId)`
- `getStudentProfile(studentId)`
- `closeConnections()`