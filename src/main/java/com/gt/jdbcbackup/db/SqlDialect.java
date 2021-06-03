package com.gt.jdbcbackup.db;

import java.util.logging.Level;
import java.util.logging.Logger;

public enum SqlDialect {
	POSTGRES,
	MYSQL,
	HSQL,
	SQLSERVER;

	public static SqlDialect find(String str) {
		
		if(str == null) {
			return null;
		}
		
		if (str.toLowerCase().startsWith("postgre")
				|| str.equals("org.postgresql.Driver")
				|| str.equals("org.postgresql.jdbc.PgConnection")) {
			return POSTGRES;
		}
		if (str.toLowerCase().startsWith("mysql")
				|| str.equals("org.mariadb.jdbc.MariaDbConnection")
				|| str.equals("com.mysql.jdbc.Driver")) {
			return MYSQL;
		}
		if (str.toLowerCase().startsWith("maria")) {
			return MYSQL;
		}
		if (str.toLowerCase().startsWith("hsql")
				|| str.equals("org.hsqldb.jdbc.JDBCConnection")) {
			return HSQL;
		}
		if (str.toLowerCase().startsWith("sqls")
				|| str.equals("com.microsoft.sqlserver.jdbc.SQLServerConnection")
				|| str.equals("com.microsoft.sqlserver.jdbc.SQLServerDriver")) {
			return SQLSERVER;
		}
		
		Logger.getLogger(SqlDialect.class.getName()).log(Level.WARNING, "no se encontró dialecto sql para '" + str + "'");

		return null;
	}
}
