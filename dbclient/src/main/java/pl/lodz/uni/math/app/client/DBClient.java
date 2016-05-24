package pl.lodz.uni.math.app.client;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DBClient {

	private static final Logger logger = LogManager.getLogger(DBClient.class);

	private Connection connection = null;

	public DBClient(String url, String user, String password) throws Exception {
		Class.forName("org.hsqldb.jdbcDriver");
		this.connection = DriverManager.getConnection(url, user, password);
	}

	public Connection getConnection() {
		return this.connection;
	}

	public void shutDown() throws SQLException {
		this.getConnection().close();
	}

	public synchronized void query(String expression, Object[] tab) throws SQLException {
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;

		preparedStatement = this.getConnection().prepareStatement(expression);
		if (tab != null && tab.length >= 1) {
			for (int i = 0; i < tab.length; i++) {
				preparedStatement.setObject(i + 1, tab[i]);
			}
		}
		resultSet = preparedStatement.executeQuery();

		PrintOutResultSet(resultSet);
		preparedStatement.close();
	}

	private void PrintOutResultSet(ResultSet rs) throws SQLException {
		ResultSetMetaData meta = rs.getMetaData();
		int amountOfCloumns = meta.getColumnCount();

		while (rs.next()) {
			StringBuffer rowAsString = new StringBuffer();
			for (int i = 0; i < amountOfCloumns; i++) {
				String format = null;
				if (i == amountOfCloumns - 1)
					format = "%s=%s";
				else
					format = "%s=%s, ";
				rowAsString.append(String.format(format, meta.getColumnLabel(i + 1), rs.getObject(i + 1).toString()));
			}
			logger.info(rowAsString);
		}
	}

	public synchronized void update(String expression) throws SQLException {
		PreparedStatement preparedStatement = this.getConnection().prepareStatement(expression);
		int rowNumber = preparedStatement.executeUpdate();
		if (rowNumber == -1) {
			logger.error("db error");
		} 
		preparedStatement.close();
	}
}
