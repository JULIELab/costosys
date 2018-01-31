package de.julielab.xmlData.dataBase;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class QueryHelper {
	/**
	 * Returns the result of a query that returns a single count
	 * 
	 * @param conn
	 * @param query
	 * @return
	 * @throws SQLException 
	 */
	public static long getCount(Connection conn, String query) throws SQLException {
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(query);
		if (rs.next())
			return rs.getLong(1);
		return -1;
	}
}
