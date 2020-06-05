/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.gt.jdbcbackup.db;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.lang3.StringUtils;

import com.gt.jdbcbackup.MainClass;

public class Backup {

	Connection connection;

	OutputStream printStream;

	public Backup() {
	}

	public static void toZipFile(String driverClass, String jdbc, String username, String password, String fileName)
			throws ClassNotFoundException, SQLException {
		java.util.logging.Logger.getLogger(MainClass.class.getName()).log(java.util.logging.Level.INFO,
				"Iniciando backup de " + jdbc);

		java.util.logging.Logger.getLogger(MainClass.class.getName()).log(java.util.logging.Level.INFO,
				"Cargando driver");
		Class.forName(driverClass);
		java.util.logging.Logger.getLogger(MainClass.class.getName()).log(java.util.logging.Level.INFO,
				"Driver cargado correctamente, conectando");

		try (Connection conn = DriverManager.getConnection(jdbc, username, password)) {
			toZipFile(conn, fileName);
		}
	}

	public static void toZipFile(Connection conn, String fileName) {

		Backup backup = new Backup();

		backup.setConnection(conn);

		try (FileOutputStream fos = new FileOutputStream(fileName + ".sql.zip");
				ZipOutputStream zos = new ZipOutputStream(fos)) {

			java.util.logging.Logger.getLogger(MainClass.class.getName()).log(java.util.logging.Level.INFO,
					"Iniciando Backup en archivo comprimido " + fileName + ".sql.zip");

			ZipEntry ze = new ZipEntry(fileName + ".sql");
			zos.putNextEntry(ze);

			backup.setPrintStream(zos);
			backup.getCreateScript();
			zos.write("\r\n".getBytes());

		} catch (FileNotFoundException ex) {
			java.util.logging.Logger.getLogger(MainClass.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
		} catch (IOException ex) {
			java.util.logging.Logger.getLogger(MainClass.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
		}

	}

	private void getCreateScript() {

		Logger.getLogger(Backup.class.getName()).log(Level.INFO, "Generando script de creacion de database");

		try {
			write("-- CREATE DATABASE " + this.connection.getCatalog() + ";\n\n");

			ResultSet rs = this.connection.getMetaData().getTables(this.connection.getCatalog(), null, "%",
					new String[] { "TABLE" });
			while (rs.next()) {
				getCreateScript(rs.getString("TABLE_SCHEM"), rs.getString("TABLE_NAME"));
			}
			write("\n\n");

			rs = this.connection.getMetaData().getTables(this.connection.getCatalog(), null, "%",
					new String[] { "TABLE" });

			while (rs.next()) {
				createInsertScript(rs.getString("TABLE_SCHEM"), rs.getString("TABLE_NAME"));
			}

			write("\n\n");
			write("-- CREATE FK " + this.connection.getCatalog() + ";\n\n");

			rs = this.connection.getMetaData().getTables(this.connection.getCatalog(), null, "%",
					new String[] { "TABLE" });
			while (rs.next()) {
				getFkScript(rs.getString("TABLE_SCHEM"), rs.getString("TABLE_NAME"));
			}

		} catch (SQLException ex) {
			Logger.getLogger(Backup.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	private void getCreateScript(String schemaName, String tableName) {
		Logger.getLogger(Backup.class.getName()).log(Level.FINE,
				"Generando script create de tabla " + schemaName + "." + tableName);

		write("-- DROP TABLE ");
		write(formatTableName(schemaName, tableName));
		write(";\n");
		write("CREATE TABLE ");
		write(formatTableName(schemaName, tableName));
		write(" (\n");
		try {
			DatabaseMetaData metaData = this.connection.getMetaData();

			ResultSet rs = metaData.getColumns(this.connection.getCatalog(), schemaName, tableName, "%");

			boolean first = true;

			boolean tieneIsAutoinc = false;
			for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
				if (rs.getMetaData().getColumnName(i).equalsIgnoreCase("IS_AUTOINCREMENT")) {
					tieneIsAutoinc = true;
					break;
				}
			}
			while (rs.next()) {
				if (first) {
					first = false;
				} else {
					write(",\n");
				}
				write(formatMysql(rs.getString("COLUMN_NAME")));
				write(" ");
				if ((tieneIsAutoinc) && (rs.getString("IS_AUTOINCREMENT") != null)
						&& (rs.getString("IS_AUTOINCREMENT").equalsIgnoreCase("yes"))) {
					switch (getConnection().getClass().getName()) {
					case "com.microsoft.sqlserver.jdbc.SQLServerDriver":
						write("IDENTITY(1,1)");
						break;
					case "org.postgresql.Driver":
						write("SERIAL");
						break;
					default:
						write(rs.getString("TYPE_NAME"));
						write("(");
						write(rs.getInt("COLUMN_SIZE") + "");
					}
				} else {
					String tipoSQL = rs.getString("TYPE_NAME");
					if (tipoSQL.equals("CLOB")) {
						tipoSQL = "TEXT";
					} else if (tipoSQL.equals("VARBINARY")) {
						switch (getConnection().getClass().getName()) {
						case "org.hsqldb.jdbcDriver":
							tipoSQL = "BLOB";
							break;
						default:
							tipoSQL = "BYTEA";
							break;
						}
					} else if (tipoSQL.equals("DOUBLE")) {
						tipoSQL = "DOUBLE PRECISION";
					}

					write(tipoSQL);

					if (tipoSQL.equals("VARCHAR")) {
						write("(");
						write(rs.getInt("COLUMN_SIZE") + "");
						write(")");
					}
				}
				if (rs.getInt("NULLABLE") == 0) {
					write(" NOT NULL");
				}
				if ((rs.getString("COLUMN_DEF") != null) && (!rs.getString("COLUMN_DEF").isEmpty())) {
					write(" DEFAULT '");
					write(rs.getString("COLUMN_DEF"));
					write("'");
				}
			}
			rs = metaData.getPrimaryKeys(this.connection.getCatalog(), schemaName, tableName);

			first = true;
			while (rs.next()) {
				if (first) {
					write(",\n");
					write("PRIMARY KEY (");
					first = false;
				} else {
					write(", ");
				}
				write(formatMysql(rs.getString("COLUMN_NAME")));
			}
			if (!first) {
				write(")");
			}
		} catch (SQLException ex) {
			Logger.getLogger(Backup.class.getName()).log(Level.SEVERE, null, ex);
			System.exit(1);
		}
		write(");\n\n");
	}

	private void getFkScript(String schemaName, String tableName) throws SQLException {
		Logger.getLogger(Backup.class.getName()).log(Level.FINE,
				"Generando script FK de tabla " + formatTableName(schemaName, tableName));

		DatabaseMetaData metaData = this.connection.getMetaData();
		ResultSet rs = metaData.getImportedKeys(this.connection.getCatalog(), schemaName, tableName);

		Map<String, String> fkTableFrom = new HashMap<String, String>();
		Map<String, String> fkTableTo = new HashMap<String, String>();
		Map<String, String> fkColumnFrom = new HashMap<String, String>();
		Map<String, String> fkColumnTo = new HashMap<String, String>();
		while (rs.next()) {
			if (!fkTableFrom.containsKey(rs.getString("FK_NAME"))) {
				fkColumnFrom.put(rs.getString("FK_NAME"), "");
				fkColumnTo.put(rs.getString("FK_NAME"), "");
				fkTableFrom.put(rs.getString("FK_NAME"),
						formatTableName(rs.getString("FKTABLE_SCHEM"), rs.getString("FKTABLE_NAME")));
				fkTableTo.put(rs.getString("FK_NAME"),
						formatTableName(rs.getString("PKTABLE_SCHEM"), rs.getString("PKTABLE_NAME")));
			}
			if (!((String) fkColumnFrom.get(rs.getString("FK_NAME"))).isEmpty()) {
				fkColumnFrom.put(rs.getString("FK_NAME"), (String) fkColumnFrom.get(tableName) + ", ");
				fkColumnTo.put(rs.getString("FK_NAME"), (String) fkColumnTo.get(tableName) + ", ");
			}
			fkColumnFrom.put(rs.getString("FK_NAME"),
					(String) fkColumnFrom.get(rs.getString("FK_NAME")) + formatMysql(rs.getString("FKCOLUMN_NAME")));
			fkColumnTo.put(rs.getString("FK_NAME"),
					(String) fkColumnTo.get(rs.getString("FK_NAME")) + formatMysql(rs.getString("PKCOLUMN_NAME")));
		}
		for (String kfk : fkTableFrom.keySet()) {
			write("ALTER TABLE ");
			write((String) fkTableFrom.get(kfk));
			write(" ADD CONSTRAINT ");
			write(formatMysql(kfk));
			write(" FOREIGN KEY (");
			write((String) fkColumnFrom.get(kfk));
			write(") ");
			write("REFERENCES ");
			write((String) fkTableTo.get(kfk));
			write("(");
			write((String) fkColumnTo.get(kfk));
			write(");\n");
		}
	}

	private List<String> getTableColumns(String schemaName, String tableName) throws SQLException {
		List<String> ret = new ArrayList<String>();

		ResultSet rs = this.connection.getMetaData().getColumns(this.connection.getCatalog(), schemaName, tableName,
				"%");
		while (rs.next()) {
			ret.add(rs.getString("COLUMN_NAME"));
		}
		return ret;
	}

	private void createInsertScript(String schemaName, String tableName) throws SQLException {

		int pos = 0;

		String columns = "";
		List<String> lstCol = getTableColumns(schemaName, tableName);

		boolean firstCol = true;
		for (String col : lstCol) {

			if (firstCol) {
				firstCol = false;
			} else {
				columns = columns + ", ";
			}
			columns = columns + formatMysql(col);
		}
		Statement stmt = this.connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		stmt.setFetchSize(100);

		ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cant FROM " + formatTableName(schemaName, tableName));

		Long total = -1L;
		Long cur = 0L;

		int cantRow = 100;
		if (lstCol.size() > 0) {
			cantRow = 50000 / Double.valueOf(Math.pow(lstCol.size(), 3.0D)).intValue();
		}
		if (cantRow < 100) {
			Logger.getLogger(Backup.class.getName()).log(Level.FINE,
					"cantidad de registros muy pequeña " + cantRow + ", cambiando a 100");
			cantRow = 100;
		}

		if (rs.next()) {
			total = rs.getLong("cant");
		}
		rs.close();
		stmt.close();

		Logger.getLogger(Backup.class.getName()).log(Level.INFO,
				"Generando insercion de datos de tabla " + formatTableName(schemaName, tableName) + ", Tomando de a "
						+ cantRow + " registros de un total de " + total);

		stmt = this.connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		stmt.setFetchSize(cantRow);

		SimpleDateFormat sqlDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

		rs = stmt.executeQuery("SELECT " + columns + " FROM " + formatTableName(schemaName, tableName));
		while (rs.next()) {
			cur++;
			if (pos == 0) {
				Logger.getLogger(Backup.class.getName()).log(Level.FINE,
						"tabla " + formatTableName(schemaName, tableName) + " registro " + cur + " de " + total);
				write(";\nINSERT INTO " + formatTableName(schemaName, tableName));
				write(" (" + columns + ") VALUES \n");
			} else {
				write(",\n");
			}
			write("(");

			firstCol = true;
			for (String col : lstCol) {

				String columnClassName = rs.getMetaData().getColumnClassName(lstCol.indexOf(col) + 1);

//				java.util.logging.Logger.getLogger(Backup.class.getName())
//				.log(java.util.logging.Level.FINE, "Columna " + col + " tipo " + columnClassName);

				if (firstCol) {
					firstCol = false;
				} else {
					write(", ");
				}
				Object val = rs.getObject(col);

				if (val == null) {
					write("NULL");
				} else {
					if (columnClassName.equals("java.sql.Clob")) {
						StringBuilder sb = new StringBuilder();

						for (Object b : (Object[]) rs.getArray(col).getArray()) {
							if (String.class.isAssignableFrom(b.getClass())) {
								if (sb.length() == 0) {
									sb.append("'");
								}
								// java.util.logging.Logger.getLogger(Backup.class.getName()).log(java.util.logging.Level.SEVERE,
								// "Me la puso en la pera, " + b);
								sb.append(b.toString().replaceAll("\'", "\'\'").replaceAll("\n", "\\\n"));
							} else {
								if (sb.length() == 0) {
									sb.append("'\\x");
								}
								sb.append(
										StringUtils.leftPad(Integer.toHexString((int) (byte) b), 2, '0').toUpperCase());
							}
						}
						sb.append("'");
						write(sb.toString());

					} else if (columnClassName.equals("java.sql.Blob")) {
						InputStream is = ((Blob) val).getBinaryStream();

						try {
							StringBuilder sb = new StringBuilder();
							sb.append("'\\x");
							int i = is.read();
							while (i != -1) {
								sb.append(StringUtils.leftPad(Integer.toHexString(i), 2, '0').toUpperCase());
								i = is.read();
							}
							sb.append("'");
							write(sb.toString());
						} catch (IOException ex) {
							// TODO Auto-generated catch block
							// ex.printStackTrace();
							java.util.logging.Logger.getLogger(Backup.class.getName())
									.log(java.util.logging.Level.SEVERE, "Error leyendo valor clob", ex);
						}
					} else if (columnClassName.equals("[B")) {
						InputStream is = new ByteArrayInputStream((byte[]) val);

						try {
							StringBuilder sb = new StringBuilder();
							sb.append("'\\x");
							int i = is.read();
							while (i != -1) {
								sb.append(StringUtils.leftPad(Integer.toHexString(i), 2, '0').toUpperCase());
								i = is.read();
							}
							sb.append("'");
							write(sb.toString());
						} catch (IOException ex) {
							// TODO Auto-generated catch block
							// ex.printStackTrace();
							java.util.logging.Logger.getLogger(Backup.class.getName())
									.log(java.util.logging.Level.SEVERE, "Error leyendo valor clob", ex);
						}

					} else if (columnClassName.equals("java.sql.Timestamp")) {
						write("'" + sqlDateFormat.format((java.sql.Timestamp) val) + "'");
					} else if (columnClassName.equals("java.sql.Date")) {
						write("'" + sqlDateFormat.format((java.sql.Date) val) + "'");
					} else {
						if (val.getClass().equals(String.class)) {
							val = "'" + val.toString().replaceAll("\'", "\'\'").replaceAll("\n", "\\\n") + "'";
						} else if (!val.getClass().equals(Integer.class) && !val.getClass().equals(Long.class)
								&& !val.getClass().equals(Double.class)
								&& !val.getClass().equals(java.math.BigDecimal.class)
								&& !val.getClass().equals(Short.class) && !val.getClass().equals(Boolean.class)) {
							java.util.logging.Logger.getLogger(Backup.class.getName()).log(
									java.util.logging.Level.WARNING, "clase desconocida " + val.getClass().getName());
						}

						write(val.toString());
					}
				}
			}
			write(")");

			pos++;
			if (pos > cantRow) {
				pos = 0;
			}
		}

		if (cur > 0) {
			write(";\n");
		}

		rs.close();
		stmt.close();
	}

	private void write(String content) {
		try {
			this.printStream.write(content.getBytes());
		} catch (IOException ex) {
			Logger.getLogger(Backup.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	public Connection getConnection() {
		return this.connection;
	}

	public void setConnection(Connection connection) {
		try {
			connection.setAutoCommit(false);
		} catch (SQLException ex) {
			Logger.getLogger(Backup.class.getName()).log(Level.SEVERE, "Error al setear autocommit en off", ex);
		}
		this.connection = connection;
	}

	public OutputStream getPrintStream() {
		return this.printStream;
	}

	public void setPrintStream(OutputStream printStream) {
		this.printStream = printStream;
	}

	private String formatTableName(String schemaName, String tableName) {
		String ret = schemaName;
		if (ret == null || ret.isEmpty()) {
			ret = "";
		} else {
			ret = formatMysql(schemaName) + ".";
		}
		ret += formatMysql(tableName);
		return ret;
	}

	private String formatMysql(String columnName) {
		if (getConnection().getClass().getName().equals("org.mariadb.jdbc.MariaDbConnection")) {

			return "`" + columnName + "`";
		}

		return columnName;
	}
}
