package pl.lodz.uni.math.app.Main;

import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.lodz.uni.math.app.client.DBClient;

public class Main {

	private static final Logger logger = LogManager.getLogger(Main.class);

	public static void main(String[] args) {
		DBClient dbClient = null;
		try {
			dbClient = new DBClient("jdbc:hsqldb:hsql://127.0.0.1:9001/test-db", "sa", "");
			createTables(dbClient);
			insertIntoTables(dbClient);
			executeQueries(dbClient);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (dbClient != null) {
				try {
					dbClient.shutDown();
				} catch (SQLException e) {
					logger.error("Error occurred while closing connection to database");
					e.printStackTrace();
				}
			}
		}
	}

	private static void createTables(DBClient dbClient) throws SQLException {
		String createStudentTableString = "CREATE TABLE Student(pkey INTEGER NOT NULL, name VARCHAR(50),"
				+ "sex VARCHAR(6) CHECK (sex='male' OR sex='female')," + "age INTEGER CHECK (age > 0),"
				+ "level INTEGER," + "PRIMARY KEY(pkey))";
		dbClient.update(createStudentTableString);

		String createFacultyTableString = "CREATE TABLE Faculty(pkey INTEGER NOT NULL, name VARCHAR(100), PRIMARY KEY(pkey))";
		dbClient.update(createFacultyTableString);

		String createClassTableString = "CREATE TABLE Class(pkey INTEGER NOT NULL, name VARCHAR(100), "
				+ "fkey_faculty INTEGER NOT NULL, PRIMARY KEY(pkey), FOREIGN KEY(fkey_faculty) REFERENCES Faculty(pkey))";
		dbClient.update(createClassTableString);

		String createEnrollmentTableString = "CREATE TABLE Enrollment(fkey_student INTEGER NOT NULL, fkey_class INTEGER NOT NULL, "
				+ "FOREIGN KEY(fkey_student) REFERENCES Student(pkey), FOREIGN KEY(fkey_class) REFERENCES Class(pkey), "
				+ "CONSTRAINT p_key_Enrollment PRIMARY KEY (fkey_student, fkey_class))";
		dbClient.update(createEnrollmentTableString);
	}

	private static void insertIntoTables(DBClient dbClient) throws SQLException {
		insertIntoStudentTable(dbClient);
		insertIntoFacultyTable(dbClient);
		insertIntoClassTable(dbClient);
		insertIntoEnrollmentTable(dbClient);
	}

	private static void insertIntoStudentTable(DBClient dbClient) throws SQLException {
		String insertIntoStudentTableString = "INSERT INTO Student VALUES" + "(1, 'John Smith', 'male', 23, 2),"
				+ "(2, 'Rebeca Milson', 'female', 27, 3)," + "(3, 'George Heartbreaker', 'male', 19, 1),"
				+ "(4, 'Deepika Chopra', 'female', 25, 3)";
		dbClient.update(insertIntoStudentTableString);
	}

	private static void insertIntoFacultyTable(DBClient dbClient) throws SQLException {
		String insertIntoFacultyTableString = "INSERT INTO Faculty VALUES" + "(100, 'Engineering'),"
				+ "(101, 'Philosophy')," + "(102, 'Law and Administration')," + "(103, 'Languages')";
		dbClient.update(insertIntoFacultyTableString);
	}

	private static void insertIntoClassTable(DBClient dbClient) throws SQLException {
		String insertIntoClassTableString = "INSERT INTO Class VALUES" + "(1000, 'Introduction to labour law', 102),"
				+ "(1001, 'Graph algorithms', 100)," + "(1002, 'Existentialism in 20th century', 101),"
				+ "(1003, 'English grammar', 103)," + "(1004, 'From Plato to Kant', 101)";
		dbClient.update(insertIntoClassTableString);
	}

	private static void insertIntoEnrollmentTable(DBClient dbClient) throws SQLException {
		String insertIntoEnrollmentTableString = "INSERT INTO Enrollment VALUES" + "(1, 1000)," + "(1, 1002),"
				+ "(1, 1003)," + "(1, 1004)," + "(2, 1002)," + "(2, 1003)," + "(4, 1000)," + "(4, 1002)," + "(4, 1003)";
		dbClient.update(insertIntoEnrollmentTableString);
	}

	private static void executeQueries(DBClient dbClient) throws SQLException {
		String queries[] = {
				"SELECT DISTINCT s.pkey as pkey, s.name as name FROM Student AS s JOIN ENROLLMENT as e ON(s.pkey = e.fkey_student)",
				"SELECT DISTINCT s.pkey as pkey, s.name as name FROM Student AS s FULL JOIN ENROLLMENT as e ON(s.pkey = e.fkey_student) WHERE e.fkey_student IS NULL AND e.fkey_class IS NULL",
				"SELECT DISTINCT s.pkey as pkey, s.name as name FROM Student AS s JOIN ENROLLMENT as e ON(s.pkey = e.fkey_student) JOIN Class as c ON (c.pkey = e.fkey_class) WHERE s.sex = 'female' AND c.name = 'Existentialism in 20th century'",
				"SELECT DISTINCT f.name FROM Faculty AS f JOIN Class AS c ON (f.pkey = c.fkey_faculty) FULL JOIN Enrollment AS e ON (c.pkey = e.fkey_class) WHERE e.fkey_student IS NULL",
				"SELECT MAX(s.age) AS MAX_age FROM Student AS s JOIN Enrollment as e ON(s.pkey = e.fkey_student) JOIN Class as c ON (c.pkey = e.fkey_class) WHERE c.name = 'Introduction to labour law'",
				"SELECT cc.name FROM Class AS cc WHERE ((SELECT COUNT(*) FROM Enrollment AS e JOIN Class AS c ON (c.pkey = e.fkey_class) WHERE c.pkey = cc.pkey GROUP BY c.pkey) >= 2)",
				"SELECT s.level AS level, AVG(s.age) as AVG_age FROM Student AS s JOIN Enrollment AS e ON (s.pkey = e.fkey_student) GROUP BY s.level"};
		for (int i = 0; i < queries.length; i++) {
			executeQuery(dbClient, queries[i], i + 1);
		}
		
	}

	private static void executeQuery(DBClient dbClient, String query, int number) throws SQLException {
		logger.info("Wynik zapytania " + number + ":");
		dbClient.query(query, null);
	}
}
