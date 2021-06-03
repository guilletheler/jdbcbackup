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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.gt.jdbcbackup.MainClass;

import org.apache.commons.lang3.StringUtils;

public class Backup {

	Connection connection;

	OutputStream printStream;

	boolean ddl = false;

	SqlDialect sqlDialect;

	SqlDialect sourceSqlDialect;

	public Backup() {
	}

	public static void toZipFile(String driverClass, String jdbc, String username, String password, String fileName,
			boolean ddl, String sqlDialect) throws ClassNotFoundException, SQLException {
		java.util.logging.Logger.getLogger(MainClass.class.getName()).log(java.util.logging.Level.INFO,
				"Iniciando backup de " + jdbc);

		java.util.logging.Logger.getLogger(MainClass.class.getName()).log(java.util.logging.Level.INFO,
				"Cargando driver");
		Class.forName(driverClass);
		java.util.logging.Logger.getLogger(MainClass.class.getName()).log(java.util.logging.Level.INFO,
				"Driver cargado correctamente, conectando");

		try (Connection conn = DriverManager.getConnection(jdbc, username, password)) {

			toZipFile(conn, fileName, ddl, sqlDialect);
		}
	}

	public static void toZipFile(Connection conn, String fileName, boolean ddl, String sqlDialectStr)
			throws SQLException {

		if (fileName == null) {
			try (ResultSet catalogs = conn.getMetaData().getCatalogs()) {
				catalogs.next();
				fileName = catalogs.getString("TABLE_CAT");
			}
		}

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss");
		fileName += sdf.format(new Date());

		Backup backup = new Backup();

		backup.ddl = ddl;

		backup.setConnection(conn);

		backup.sourceSqlDialect = SqlDialect.find(conn.getClass().getName());

		backup.sqlDialect = SqlDialect.find(sqlDialectStr);

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
			java.util.logging.Logger.getLogger(MainClass.class.getName()).log(java.util.logging.Level.SEVERE,
					"Archivo no encontrado", ex);
		} catch (IOException ex) {
			java.util.logging.Logger.getLogger(MainClass.class.getName()).log(java.util.logging.Level.SEVERE,
					"Error de entrada/salida", ex);
		}

	}

	private void getCreateScript() {

		Logger.getLogger(Backup.class.getName()).log(Level.INFO, "Generando script de creacion de database");

		try {
			createTablesScript();

			if (!ddl) {
				createInsertsScript();
			}

			createIndexesScript();

			createFKsScript();

			createAutoIncNextValues();

		} catch (SQLException ex) {
			Logger.getLogger(Backup.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	private void createIndexesScript() throws SQLException {
		write("-- CREATE INDEX " + this.connection.getCatalog() + ";\n\n");

		try (ResultSet rs = this.connection.getMetaData().getTables(this.connection.getCatalog(), null, "%",
				new String[] { "TABLE" })) {

			while (rs.next()) {
				createIndexScript(this.connection.getMetaData().getIndexInfo(null, rs.getString("TABLE_SCHEM"),
						rs.getString("TABLE_NAME"), false, false));

			}
		}
	}

	private void createIndexScript(ResultSet rs) throws SQLException {

		Map<String, String> indexDef = new HashMap<>();

		String table = null;

		while (rs.next()) {

			if (rs.getString("INDEX_NAME") == null || rs.getString("INDEX_NAME").equals("null")) {
				continue;
			}

			if (rs.getString("TABLE_NAME") == null || rs.getString("TABLE_NAME").equals("null")) {
				continue;
			}

			switch (getSourceSqlDialect()) {
				case HSQL:
					if (rs.getString("INDEX_NAME").startsWith("SYS_IDX_SYS_PK_")
							|| rs.getString("INDEX_NAME").startsWith("SYS_IDX_FK")) {
						continue;
					}
					break;
				case MYSQL:
				case SQLSERVER:
				case POSTGRES:
				default:
			}

			if (table == null) {
				table = this.formatTableName(rs.getString("TABLE_SCHEM"), rs.getString("TABLE_NAME"));
			}

			if (indexDef.containsKey(rs.getString("INDEX_NAME"))) {
				indexDef.put(rs.getString("INDEX_NAME"),
						indexDef.get(rs.getString("INDEX_NAME")) + ", " + rs.getString("COLUMN_NAME"));
			} else {
				String idx = "CREATE ";
				if (!rs.getBoolean("NON_UNIQUE")) {
					idx += "UNIQUE ";
				}
				idx += "INDEX " + this.formatMysql(rs.getString("INDEX_NAME")) + "\n\tON ";

				idx += this.formatTableName(rs.getString("TABLE_SCHEM"), rs.getString("TABLE_NAME")) + "("
						+ this.formatMysql(rs.getString("COLUMN_NAME"));

				indexDef.put(rs.getString("INDEX_NAME"), idx);
			}

			Logger.getLogger(getClass().getName()).log(Level.INFO,
					"indice para " + rs.getString("TABLE_SCHEM") + "." + rs.getString("TABLE_NAME") + " "
							+ rs.getBoolean("NON_UNIQUE") + " " + rs.getString("INDEX_QUALIFIER") + " "
							+ rs.getString("INDEX_NAME") + " " + rs.getString("TYPE") + " "
							+ rs.getShort("ORDINAL_POSITION") + " " + rs.getString("COLUMN_NAME") + " "
							+ rs.getString("ASC_OR_DESC") + " " + rs.getLong("CARDINALITY") + " " + rs.getLong("PAGES")
							+ " " + rs.getString("FILTER_CONDITION"));
		}

		if (!indexDef.isEmpty()) {
			write("-- CREATE INDEX TABLA " + table + "\n\n");

			for (Map.Entry<String, String> entry : indexDef.entrySet()) {
				write(entry.getValue() + ");\n\n");
			}
		}

	}

	private void createFKsScript() throws SQLException {

		write("-- CREATE FK " + this.connection.getCatalog() + ";\n\n");

		try (ResultSet rs = this.connection.getMetaData().getTables(this.connection.getCatalog(), null, "%",
				new String[] { "TABLE" })) {
			while (rs.next()) {
				getFkScript(rs.getString("TABLE_SCHEM"), rs.getString("TABLE_NAME"));
			}
		}
	}

	private void createInsertsScript() throws SQLException {
		try (ResultSet rs = this.connection.getMetaData().getTables(this.connection.getCatalog(), null, "%",
				new String[] { "TABLE" })) {

			while (rs.next()) {
				createInsertScript(rs.getString("TABLE_SCHEM"), rs.getString("TABLE_NAME"));
			}

		}

		write("\n\n");
	}

	private void createTablesScript() throws SQLException {
		write("-- CREATE DATABASE " + this.connection.getCatalog() + ";\n\n");

		try (ResultSet rs = this.connection.getMetaData().getTables(this.connection.getCatalog(), null, "%",
				new String[] { "TABLE" })) {
			while (rs.next()) {
				createTableScript(rs.getString("TABLE_SCHEM"), rs.getString("TABLE_NAME"));
			}
		}
		write("\n\n");
	}

	private void createAutoIncNextValues() throws SQLException {
		write("-- CREATE AUTO INCREMENT NEXT VALUES " + this.connection.getCatalog() + ";\n\n");

		try (ResultSet rs = this.connection.getMetaData().getTables(this.connection.getCatalog(), null, "%",
				new String[] { "TABLE" })) {
			while (rs.next()) {
				createAutoIncNextValue(rs.getString("TABLE_SCHEM"), rs.getString("TABLE_NAME"));
			}
		}
		write("\n\n");
	}

	private void enableIdentityInsert(String schemaName, String tableName) throws SQLException {
		Logger.getLogger(Backup.class.getName()).log(Level.FINE,
				"Generando auto inc next value de tabla " + schemaName + "." + tableName);

		DatabaseMetaData metaData = this.connection.getMetaData();

		try (ResultSet rs = metaData.getColumns(this.connection.getCatalog(), schemaName, tableName, "%")) {
			while (rs.next()) {
				if ((rs.getString("IS_AUTOINCREMENT") != null)
						&& (rs.getString("IS_AUTOINCREMENT").equalsIgnoreCase("yes"))) {
					switch (getSqlDialect()) {
						case MYSQL:
						case HSQL:
						case POSTGRES:
							break;
						case SQLSERVER:
						write("\n\nSET IDENTITY_INSERT ");
						write(formatTableName(schemaName, tableName));
						write(" ON; --\n");
							break;
					}
				}
			}
		}
	}

	private void disableIdentityInsert(String schemaName, String tableName) throws SQLException {
		Logger.getLogger(Backup.class.getName()).log(Level.FINE,
				"Generando auto inc next value de tabla " + schemaName + "." + tableName);

		DatabaseMetaData metaData = this.connection.getMetaData();

		try (ResultSet rs = metaData.getColumns(this.connection.getCatalog(), schemaName, tableName, "%")) {
			while (rs.next()) {
				if ((rs.getString("IS_AUTOINCREMENT") != null)
						&& (rs.getString("IS_AUTOINCREMENT").equalsIgnoreCase("yes"))) {
					switch (getSqlDialect()) {
						case MYSQL:
						case HSQL:
						case POSTGRES:
							break;
						case SQLSERVER:
						write("\nSET IDENTITY_INSERT ");
						write(formatTableName(schemaName, tableName));
						write(" OFF;\n\n");
							break;
					}
				}
			}
		}
	}

	private void createAutoIncNextValue(String schemaName, String tableName) throws SQLException {
		Logger.getLogger(Backup.class.getName()).log(Level.FINE,
				"Generando auto inc next value de tabla " + schemaName + "." + tableName);

		DatabaseMetaData metaData = this.connection.getMetaData();

		try (ResultSet rs = metaData.getColumns(this.connection.getCatalog(), schemaName, tableName, "%")) {
			while (rs.next()) {
				if ((rs.getString("IS_AUTOINCREMENT") != null)
						&& (rs.getString("IS_AUTOINCREMENT").equalsIgnoreCase("yes"))) {
					Long nextValue = getNextValue(schemaName, tableName, rs.getString("COLUMN_NAME"));

					switch (getSqlDialect()) {
						case MYSQL:
							write("ALTER TABLE " + formatTableName(schemaName, tableName) + " AUTO_INCREMENT = "
									+ nextValue + ";\n\n");
							write("ALTER TABLE " + formatTableName(schemaName, tableName) + " AUTO_INCREMENT = "
									+ nextValue + ";\n\n");
							break;
						case HSQL:
							write("ALTER TABLE " + formatTableName(schemaName, tableName) + "ALTER COLUMN "
									+ rs.getString("COLUMN_NAME") + " RESTART WITH " + nextValue + ";\n\n");
							break;
						case POSTGRES:
							write("SELECT setval(pg_get_serial_sequence('" + formatTableName(schemaName, tableName)
									+ "', '" + rs.getString("COLUMN_NAME") + "'), coalesce(max("
									+ rs.getString("COLUMN_NAME") + "),0) + 1, false) FROM "
									+ formatTableName(schemaName, tableName) + ";\n\n");
						case SQLSERVER:
							write("DBCC checkident ('" + formatTableName(schemaName, tableName) + "', reseed);\n\n");
							break;
						default:
							break;
					}
				}
			}
		}

	}

	private Long getNextValue(String schemaName, String tableName, String colName) throws SQLException {

		String sqlCmd = "SELECT MAX(COALESCE(" + formatSourceMysql(colName) + ", 0)) + 1 as max " + "FROM "
				+ formatSourceTableName(schemaName, tableName);

		Long ret = 1L;

		try (PreparedStatement stmt = this.getConnection().prepareStatement(sqlCmd);
				ResultSet rs = stmt.executeQuery()) {

			rs.next();

			ret = rs.getLong(1);
		}

		return ret;
	}

	private void createTableScript(String schemaName, String tableName) throws SQLException {
		Logger.getLogger(Backup.class.getName()).log(Level.FINE,
				"Generando script create de tabla " + schemaName + "." + tableName);

		write("-- DROP TABLE ");
		write(formatTableName(schemaName, tableName));
		write(";\n");
		write("CREATE TABLE ");
		write(formatTableName(schemaName, tableName));
		write(" (\n");
		DatabaseMetaData metaData = this.connection.getMetaData();

		try (ResultSet rs = metaData.getColumns(this.connection.getCatalog(), schemaName, tableName, "%")) {

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
				write("\t");

				write(formatMysql(rs.getString("COLUMN_NAME")));
				write(" ");
				if ((tieneIsAutoinc) && (rs.getString("IS_AUTOINCREMENT") != null)
						&& (rs.getString("IS_AUTOINCREMENT").equalsIgnoreCase("yes"))) {
					switch (getSqlDialect()) {
						case SQLSERVER:
							write(rs.getString("TYPE_NAME").toUpperCase());
							break;
						case POSTGRES:
							write("SERIAL");
							break;
						case HSQL:
							write(rs.getString("TYPE_NAME"));
							write(" IDENTITY");
							break;
						case MYSQL:
							write(rs.getString("TYPE_NAME"));
							write(" AUTO_INCREMENT");
							break;
						default:
							Logger.getLogger(getClass().getName()).log(Level.INFO,
									"no se encuentra script para autoincrement de columna "
											+ rs.getString("COLUMN_NAME") + " driver "
											+ getConnection().getClass().getName());
							write(rs.getString("TYPE_NAME"));
							write("(");
							write(rs.getInt("COLUMN_SIZE") + "");
							write(")");
					}
				} else {
					String tipoSQL = rs.getString("TYPE_NAME").toUpperCase();
					if (tipoSQL.equals("CLOB")) {
						tipoSQL = "TEXT";
					} else if (tipoSQL.equals("VARBINARY") || tipoSQL.equals("BLOB")) {
						switch (getSqlDialect()) {
							case HSQL:
								tipoSQL = "BLOB";
								break;
							case MYSQL:
								if (rs.getInt("COLUMN_SIZE") < 256) {
									tipoSQL = "TINYBLOB";
								} else if (rs.getInt("COLUMN_SIZE") < 65536) {
									tipoSQL = "BLOB";
								} else if (rs.getInt("COLUMN_SIZE") < 16777216) {
									tipoSQL = "MEDIUMBLOB";
								} else {
									tipoSQL = "LONGBLOB";
								}
								break;
							case POSTGRES:
							case SQLSERVER:
							default:
								tipoSQL = "BYTEA";
								break;
						}
					} else if (tipoSQL.equals("DOUBLE")) {
						tipoSQL = "DOUBLE PRECISION";
					}

					if (tipoSQL.equals("VARCHAR") && rs.getInt("COLUMN_SIZE") > 1000) {
						switch (getSqlDialect()) {
							case HSQL:
								break;
							case MYSQL:
							case POSTGRES:
							case SQLSERVER:
								tipoSQL = "TEXT";
						}
					}

					write(tipoSQL);

					if ((tipoSQL.equals("VARCHAR") || tipoSQL.equals("CHAR") || tipoSQL.equals("DECIMAL")
							|| tipoSQL.equals("NUMERIC")) && rs.getInt("COLUMN_SIZE") > 0) {
						write("(");
						write(rs.getInt("COLUMN_SIZE") + "");
						if (rs.getInt("DECIMAL_DIGITS") > 0) {
							write(",");
							write(rs.getInt("DECIMAL_DIGITS") + "");
						}
						write(")");
					}

				}

				switch (getSqlDialect()) {
					case HSQL:
						if ((rs.getString("COLUMN_DEF") != null) && (!rs.getString("COLUMN_DEF").isEmpty())) {
							write(" DEFAULT '");
							write(rs.getString("COLUMN_DEF"));
							write("'");
						}
						if (rs.getInt("NULLABLE") == 0) {
							write(" NOT NULL");
						}
						break;
					case POSTGRES:
					case SQLSERVER:
					case MYSQL:
					default:
						if (rs.getInt("NULLABLE") == 0) {
							write(" NOT NULL");
						}
						if ((rs.getString("COLUMN_DEF") != null) && (!rs.getString("COLUMN_DEF").isEmpty())) {

							String defVal = rs.getString("COLUMN_DEF");

							if (rs.getString("TYPE_NAME").equalsIgnoreCase("TEXT")
									|| rs.getString("TYPE_NAME").equalsIgnoreCase("VARCHAR")) {
								if (!defVal.startsWith("'")) {
									defVal = "'" + defVal;
								}
								if (!defVal.endsWith("'")) {
									defVal = defVal + "'";
								}
							}

							write(" DEFAULT ");
							write(defVal);
						}
						break;
				}
			}
		}
		try (ResultSet rs = metaData.getPrimaryKeys(this.connection.getCatalog(), schemaName, tableName)) {

			boolean first = true;
			while (rs.next()) {
				if (first) {
					write(",\n");
					write("\t");
					write("PRIMARY KEY (");
					first = false;
				} else {
					write(", ");
				}
				write(formatMysql(rs.getString("COLUMN_NAME")));
			}
			if (!first) {
				write(")\n");
			}
		}

		write(");\n\n");
	}

	private void getFkScript(String schemaName, String tableName) throws SQLException {
		Logger.getLogger(Backup.class.getName()).log(Level.FINE,
				"Generando script FK de tabla " + formatTableName(schemaName, tableName));

		DatabaseMetaData metaData = this.connection.getMetaData();
		try (ResultSet rs = metaData.getImportedKeys(this.connection.getCatalog(), schemaName, tableName)) {

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
				fkColumnFrom.put(rs.getString("FK_NAME"), (String) fkColumnFrom.get(rs.getString("FK_NAME"))
						+ formatMysql(rs.getString("FKCOLUMN_NAME")));
				fkColumnTo.put(rs.getString("FK_NAME"),
						(String) fkColumnTo.get(rs.getString("FK_NAME")) + formatMysql(rs.getString("PKCOLUMN_NAME")));
			}
			for (String kfk : fkTableFrom.keySet()) {
				write("ALTER TABLE ");
				write((String) fkTableFrom.get(kfk));
				write("\n\tADD CONSTRAINT ");
				write(formatMysql(kfk));
				write("\n\tFOREIGN KEY (");
				write((String) fkColumnFrom.get(kfk));
				write(") ");
				write("\n\tREFERENCES ");
				write((String) fkTableTo.get(kfk));
				write("(");
				write((String) fkColumnTo.get(kfk));
				write(");\n\n");
			}
		}
	}

	private List<String> getTableColumns(String schemaName, String tableName) throws SQLException {
		List<String> ret = new ArrayList<String>();

		try (ResultSet rs = this.connection.getMetaData().getColumns(this.connection.getCatalog(), schemaName,
				tableName, "%")) {
			while (rs.next()) {
				ret.add(rs.getString("COLUMN_NAME"));
			}
		}
		return ret;
	}

	private void createInsertScript(String schemaName, String tableName) throws SQLException {

		int pos = 0;

		String sourceColumns = "";
		String columns = "";
		List<String> lstCol = getTableColumns(schemaName, tableName);

		boolean firstCol = true;
		for (String col : lstCol) {

			if (firstCol) {
				firstCol = false;
			} else {
				columns = columns + ", ";
				sourceColumns = sourceColumns + ", ";
			}
			sourceColumns = sourceColumns + formatSourceMysql(col);
			columns = columns + formatMysql(col);
		}
		Statement stmt = this.connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		stmt.setFetchSize(100);

		ResultSet rs = stmt
				.executeQuery("SELECT COUNT(*) AS cant FROM " + formatSourceTableName(schemaName, tableName));

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
		if (getSqlDialect() == SqlDialect.SQLSERVER && cantRow > 1000) {
			cantRow = 999;
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

		String sqlSelect = "SELECT " + sourceColumns + " FROM " + formatSourceTableName(schemaName, tableName);

		Logger.getLogger(Backup.class.getName()).log(Level.INFO, "Obteniendo datos con\n" + sqlSelect);

		rs = stmt.executeQuery(sqlSelect);
		while (rs.next()) {
			cur++;
			if (pos == 0) {
				Logger.getLogger(Backup.class.getName()).log(Level.FINE,
						"tabla " + formatTableName(schemaName, tableName) + " registro " + cur + " de " + total);
				enableIdentityInsert(schemaName, tableName);
				write(";\nINSERT INTO " + formatTableName(schemaName, tableName));
				write(" (" + columns + ") VALUES \n");
			} else {
				write(",\n");
			}
			write("(");

			firstCol = true;
			for (String col : lstCol) {

				String columnClassName = rs.getMetaData().getColumnClassName(lstCol.indexOf(col) + 1);

				// java.util.logging.Logger.getLogger(Backup.class.getName())
				// .log(java.util.logging.Level.FINE, "Columna " + col + " tipo " +
				// columnClassName);

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
					} else if (val.getClass().equals(Boolean.class)) {
						switch (getSqlDialect()) {
							case SQLSERVER:
								write(((boolean) val) ? "1" : "0");
								break;
							case HSQL:
							case POSTGRES:
							case MYSQL:
							default:
								write(val.toString());
								break;
						}
					} else {
						if (val.getClass().equals(String.class)) {
							val = "'" + val.toString().replaceAll("\'", "\'\'").replaceAll("\n", "\\\n") + "'";
						} else if (!val.getClass().equals(Integer.class) && !val.getClass().equals(Long.class)
								&& !val.getClass().equals(Double.class)
								&& !val.getClass().equals(java.math.BigDecimal.class)
								&& !val.getClass().equals(Short.class)) {
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

		disableIdentityInsert(schemaName, tableName);
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
		if (getSqlDialect() == SqlDialect.MYSQL) {

			ret = "`" + tableName + "`";
		} else {
			if (ret == null || ret.isEmpty()) {
				ret = "";
			} else {
				ret = schemaName + ".";
			}
			ret += tableName;
		}
		return ret;
	}

	private String formatSourceTableName(String schemaName, String tableName) {
		String ret = schemaName;
		if (getSourceSqlDialect() == SqlDialect.MYSQL) {

			ret = "`" + tableName + "`";
		} else {
			if (ret == null || ret.isEmpty()) {
				ret = "";
			} else {
				ret = schemaName + ".";
			}
			ret += tableName;
		}
		return ret;
	}

	private String formatSourceMysql(String columnName) {
		if (getSourceSqlDialect() == SqlDialect.MYSQL) {

			return "`" + columnName + "`";
		}

		return columnName;
	}

	private String formatMysql(String columnName) {
		if (getSqlDialect() == SqlDialect.MYSQL) {

			return "`" + columnName + "`";
		}

		return columnName;
	}

	public SqlDialect getSourceSqlDialect() {
		return sourceSqlDialect;
	}

	public SqlDialect getSqlDialect() {
		if (sqlDialect == null) {
			sqlDialect = SqlDialect.find(getConnection().getClass().getName());
		}
		return sqlDialect;
	}

	public void setSqlDialect(SqlDialect sqlDialect) {
		this.sqlDialect = sqlDialect;
	}
}
