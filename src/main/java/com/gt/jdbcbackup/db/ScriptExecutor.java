package com.gt.jdbcbackup.db;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Scanner;

import com.gt.jdbcbackup.MainClass;

public class ScriptExecutor {

	Connection connection;

	String fileName;

	public ScriptExecutor() {
	}

	public static void runFile(String driverClass, String jdbc, String username, String password, String fileName)
			throws ClassNotFoundException, SQLException {
		java.util.logging.Logger.getLogger(MainClass.class.getName()).log(java.util.logging.Level.INFO,
				"Iniciando backup de " + jdbc);

		java.util.logging.Logger.getLogger(MainClass.class.getName()).log(java.util.logging.Level.INFO,
				"Cargando driver");
		Class.forName(driverClass);
		java.util.logging.Logger.getLogger(MainClass.class.getName()).log(java.util.logging.Level.INFO,
				"Driver cargado correctamente, conectando");

		try (Connection conn = DriverManager.getConnection(jdbc, username, password)) {
			runFile(conn, fileName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void runFile(Connection conn, String fileName) throws IOException, SQLException {
		ScriptExecutor se = new ScriptExecutor();
		se.setFileName(fileName);
		se.setConnection(conn);
		se.run();
	}

	public void run() throws IOException, SQLException {
		FileInputStream inputStream = null;
		Scanner sc = null;
		StringBuilder sb = new StringBuilder();
		String line;
		try {
			inputStream = new FileInputStream(fileName);
			sc = new Scanner(inputStream, "UTF-8");
			while (sc.hasNextLine()) {
				line = sc.nextLine();

				if (line.trim().isEmpty() || line.startsWith("--") || line.startsWith("#") || line.equals(";")
						|| line.equals("//")) {
					continue;
				}
				if (sb.length() > 0) {
					sb.append("\n");
				}
				sb.append(line);

				if (line.endsWith(";")) {
					runStatement(sb.toString());
					sb = new StringBuilder();
				}

			}

			line = sb.toString();

			if (!(line.trim().isEmpty() || line.startsWith("--") || line.startsWith("#") || line.equals(";")
					|| line.equals("//"))) {
				runStatement(line);
			}

			// note that Scanner suppresses exceptions
			if (sc.ioException() != null) {
				throw sc.ioException();
			}
		} finally {
			if (inputStream != null) {
				inputStream.close();
			}
			if (sc != null) {
				sc.close();
			}
		}

	}

	private void runStatement(String statement) throws SQLException {
		statement = statement.trim();
		try (PreparedStatement pstmt = this.getConnection().prepareStatement(statement)) {
			System.out.println("Ejecutando: " + statement);
			pstmt.execute();
		}

	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public Connection getConnection() {
		return connection;
	}

	public void setConnection(Connection connection) {
		this.connection = connection;
	}

}
