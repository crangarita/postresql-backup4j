package com.github.ludoviccarretti.services;

import com.github.ludoviccarretti.model.InformationSchemaGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeroturnaround.zip.ZipUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import static com.github.ludoviccarretti.options.PropertiesOptions.*;

/**
 * Created by lcarretti on 30-Sep-19.
 */
public class PostgresqlExportService {

    private Statement stmt;
    private String database;
    private String generatedSql = "";
    private Logger logger = LoggerFactory.getLogger(getClass());
    private final String LOG_PREFIX = "java-postgresql-exporter";
    private String dirName = "java-postgresql-exporter-temp";
    private String sqlFileName = "";
    private String zipFileName = "";
    private Properties properties;
    private File generatedZipFile;

    public PostgresqlExportService(Properties properties) {
        this.properties = properties;
    }

    /**
     * This function will check if the required minimum
     * properties are set for database connection and exporting
     *
     * @return bool
     */
    private boolean isValidateProperties() {
        return properties != null &&
                properties.containsKey(DB_USERNAME) &&
                properties.containsKey(DB_PASSWORD) &&
                (properties.containsKey(DB_NAME) || properties.containsKey(JDBC_CONNECTION_STRING));
    }

    /**
     * This function will check if all the minimum
     * required email properties are set,
     * that can facilitate sending of exported
     * sql to email
     *
     * @return bool
     */
    private boolean isEmailPropertiesSet() {
        return properties != null &&
                properties.containsKey(EMAIL_HOST) &&
                properties.containsKey(EMAIL_PORT) &&
                properties.containsKey(EMAIL_USERNAME) &&
                properties.containsKey(EMAIL_PASSWORD) &&
                properties.containsKey(EMAIL_FROM) &&
                properties.containsKey(EMAIL_TO);
    }

    /**
     * This function will return true
     * or false based on the availability
     * or absence of a custom output sql
     * file name
     *
     * @return bool
     */
    private boolean isSqlFileNamePropertySet() {
        return properties != null &&
                properties.containsKey(SQL_FILE_NAME);
    }

    /**
     * This will generate the SQL statement
     * for creating the table supplied in the
     * method signature
     *
     * @param sequence the sequence concerned
     * @return String
     */
    private String getSequenceInsertStatement(InformationSchemaGenerator sequence) {
        StringBuilder sql = new StringBuilder();
        boolean addIfNotExists = Boolean.parseBoolean(properties.containsKey(ADD_IF_NOT_EXISTS) ? properties.getProperty(ADD_IF_NOT_EXISTS, "true") : "true");

        if (sequence != null) {
            String query = sequence.toSQL();
            sql.append("\n\n--");
            sql.append("\n").append(PostgresqlBaseService.SQL_START_PATTERN).append("  sequence dump : ").append(sequence.getName());
            sql.append("\n--\n\n");

            if (addIfNotExists) {
                query = query.trim().replace("CREATE SEQUENCE", "CREATE SEQUENCE IF NOT EXISTS");
            }

            sql.append(query);
            sql.append("\n\n--");
            sql.append("\n").append(PostgresqlBaseService.SQL_END_PATTERN).append("  sequence dump : ").append(sequence.getName());
            sql.append("\n--\n\n");
        }
        return sql.toString();
    }

    /**
     * This will generate the SQL statement
     * for creating the table supplied in the
     * method signature
     *
     * @param table the table concerned
     * @return String
     */
    private String getTableInsertStatement(InformationSchemaGenerator table) {
        StringBuilder sql = new StringBuilder();
        boolean addIfNotExists = Boolean.parseBoolean(properties.containsKey(ADD_IF_NOT_EXISTS) ? properties.getProperty(ADD_IF_NOT_EXISTS, "true") : "true");

        if (table != null) {
            String query = table.toSQL();
            sql.append("\n\n--");
            sql.append("\n").append(PostgresqlBaseService.SQL_START_PATTERN).append("  table dump : ").append(table.getName());
            sql.append("\n--\n\n");

            if (addIfNotExists) {
                query = query.trim().replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");
            }

            sql.append(query);
            sql.append("\n\n--");
            sql.append("\n").append(PostgresqlBaseService.SQL_END_PATTERN).append("  table dump : ").append(table.getName());
            sql.append("\n--\n\n");
        }
        return sql.toString();
    }


    /**
     * This function will generate the insert statements needed
     * to recreate the table under processing.
     *
     * @param table the table to get inserts statement for
     * @return String generated SQL insert
     * @throws SQLException exception
     */
    private String getDataInsertStatement(String table) throws SQLException {

        StringBuilder sql = new StringBuilder();

        ResultSet rs = stmt.executeQuery("SELECT * FROM " + table + ";");

        //move to the last row to get max rows returned
        rs.last();
        int rowCount = rs.getRow();

        //there are no records just return empty string
        if (rowCount <= 0) {
            return sql.toString();
        }

        sql.append("\n--").append("\n-- Inserts of ").append(table).append("\n--\n\n");

        sql.append("\n--\n")
                .append(PostgresqlBaseService.SQL_START_PATTERN).append(" table insert : ").append(table)
                .append("\n--\n");

        sql.append("INSERT INTO \"").append(table).append("\" (");

        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        //generate the column names that are present
        //in the returned result set
        //at this point the insert is INSERT INTO (`col1`, `col2`, ...)
        for (int i = 0; i < columnCount; i++) {
            sql.append("\"")
                    .append(metaData.getColumnName(i + 1))
                    .append("\", ");
        }

        //remove the last whitespace and comma
        sql.deleteCharAt(sql.length() - 1).deleteCharAt(sql.length() - 1).append(") VALUES \n");

        //now we're going to build the values for data insertion
        rs.beforeFirst();
        while (rs.next()) {
            sql.append("(");
            for (int i = 0; i < columnCount; i++) {

                int columnType = metaData.getColumnType(i + 1);
                int columnIndex = i + 1;

                //this is the part where the values are processed based on their type
                if (Objects.isNull(rs.getObject(columnIndex))) {
                    sql.append(rs.getObject(columnIndex)).append(", ");
                } else if (columnType == Types.INTEGER || columnType == Types.TINYINT) {
                    sql.append(rs.getInt(columnIndex)).append(", ");
                } else if (columnType == Types.BIT) {
                    sql.append(rs.getBoolean(columnIndex)).append(", ");
                } else if (columnType == Types.BIGINT) {
                    sql.append(rs.getLong(columnIndex)).append(", ");
                } else {

                    String val = rs.getString(columnIndex);
                    //escape the single quotes that might be in the value
                    val = val.replace("'", "\\'");

                    sql.append("'").append(val).append("', ");
                }
            }

            //now that we're done with a row
            //let's remove the last whitespace and comma
            sql.deleteCharAt(sql.length() - 1).deleteCharAt(sql.length() - 1);

            //if this is the last row, just append a closing
            //parenthesis otherwise append a closing parenthesis and a comma
            //for the next set of values
            if (rs.isLast()) {
                sql.append(")");
            } else {
                sql.append("),\n");
            }
        }

        //now that we are done processing the entire row
        //let's add the terminator
        sql.append(";");

        sql.append("\n--\n")
                .append(PostgresqlBaseService.SQL_END_PATTERN).append(" table insert : ").append(table)
                .append("\n--\n");

        return sql.toString();
    }


    /**
     * This is the entry function that'll
     * coordinate getTableInsertStatement() and getDataInsertStatement()
     * for every table in the database to generate a whole
     * script of SQL
     *
     * @return String
     * @throws SQLException exception
     */
    private String exportToSql() throws SQLException {

        StringBuilder sql = new StringBuilder();
        sql.append("--");
        sql.append("\n-- Generated by postgresql-backup4j");
        sql.append("\n-- https://github.com/ludoviccarretti/postresql-backup4j");
        sql.append("\n-- Date: ").append(new SimpleDateFormat("d-M-Y H:m:s").format(new Date()));
        sql.append("\n--");

        // Create postgres utility function
        PostgresqlBaseService.createPostgresSqlFunction(stmt);


        // get all sequences that are in the database
        try {
            PostgresqlBaseService.getAllSequences(stmt).forEach(sequence -> {
                sql.append(getSequenceInsertStatement(sequence));
            });
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //get the tables that are in the database
        List<InformationSchemaGenerator> tables = PostgresqlBaseService.getAllTables(stmt);

        //for every table, get the table creation and data
        // insert statement
        for (InformationSchemaGenerator s : tables) {
            try {
                sql.append(getTableInsertStatement(s));
                sql.append(getDataInsertStatement(s.getName().trim()));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        // Delete all utility functions
        PostgresqlBaseService.deletePostgresSqlFunction(stmt);


        this.generatedSql = sql.toString();
        return sql.toString();
    }

    /**
     * This is the entry point for exporting
     * the database. It performs validation and
     * the initial object initializations,
     * database connection and setup
     * before ca
     *
     * @throws IOException            exception
     * @throws SQLException           exception
     * @throws ClassNotFoundException exception
     */
    public void export() throws IOException, SQLException, ClassNotFoundException {

        //check if properties is set or not
        if (!isValidateProperties()) {
            logger.error("Invalid config properties: The config properties is missing important parameters: DB_NAME, DB_USERNAME and DB_PASSWORD");
            return;
        }

        //connect to the database
        database = properties.getProperty(DB_NAME);
        String jdbcURL = properties.getProperty(JDBC_CONNECTION_STRING, "");
        String driverName = properties.getProperty(JDBC_DRIVER_NAME, "");

        Connection connection;

        if (jdbcURL.isEmpty()) {
            connection = PostgresqlBaseService.connect(properties.getProperty(DB_USERNAME), properties.getProperty(DB_PASSWORD),
                    database, driverName);
        } else {
            if (jdbcURL.contains("?")) {
                database = jdbcURL.substring(jdbcURL.lastIndexOf("/") + 1, jdbcURL.indexOf("?"));
            } else {
                database = jdbcURL.substring(jdbcURL.lastIndexOf("/") + 1);
            }
            logger.debug("database name extracted from connection string: " + database);
            connection = PostgresqlBaseService.connectWithURL(properties.getProperty(DB_USERNAME), properties.getProperty(DB_PASSWORD),
                    jdbcURL, driverName);
        }

        stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);

        //generate the final SQL
        String sql = exportToSql();

        //create a temp dir to store the exported file for processing
        dirName = properties.getProperty(TEMP_DIR, dirName);
        File file = new File(dirName);
        if (!file.exists()) {
            boolean res = file.mkdir();
            if (!res) {
                throw new IOException(LOG_PREFIX + ": Unable to create temp dir: " + file.getAbsolutePath());
            }
        }

        //write the sql file out
        File sqlFolder = new File(dirName + "/sql");
        if (!sqlFolder.exists()) {
            boolean res = sqlFolder.mkdir();
            if (!res) {
                throw new IOException(LOG_PREFIX + ": Unable to create temp dir: " + file.getAbsolutePath());
            }
        }

        sqlFileName = getSqlFilename();
        FileOutputStream outputStream = new FileOutputStream(sqlFolder + "/" + sqlFileName);
        outputStream.write(sql.getBytes());
        outputStream.close();

        //zip the file
        zipFileName = dirName + "/" + sqlFileName.replace(".sql", ".zip");
        generatedZipFile = new File(zipFileName);
        ZipUtil.pack(sqlFolder, generatedZipFile);

        //mail the zipped file if mail settings are available
        if (isEmailPropertiesSet()) {
            boolean emailSendingRes = EmailService.builder()
                    .setHost(properties.getProperty(EMAIL_HOST))
                    .setPort(Integer.parseInt(properties.getProperty(EMAIL_PORT)))
                    .setToAddress(properties.getProperty(EMAIL_TO))
                    .setFromAddress(properties.getProperty(EMAIL_FROM))
                    .setUsername(properties.getProperty(EMAIL_USERNAME))
                    .setPassword(properties.getProperty(EMAIL_PASSWORD))
                    .setSubject(properties.getProperty(EMAIL_SUBJECT, sqlFileName.replace(".sql", "").toUpperCase()))
                    .setMessage(properties.getProperty(EMAIL_MESSAGE, "Please find attached database backup of " + database))
                    .setAttachments(new File[]{new File(zipFileName)})
                    .sendMail();

            if (emailSendingRes) {
                logger.debug(LOG_PREFIX + ": Zip File Sent as Attachment to Email Address Successfully");
            } else {
                logger.error(LOG_PREFIX + ": Unable to send zipped file as attachment to email. See log debug for more info");
            }
        }

        //clear the generated temp files
        clearTempFiles(Boolean.parseBoolean(properties.getProperty(PRESERVE_GENERATED_ZIP, Boolean.FALSE.toString())));

    }

    /**
     * This function will delete all the
     * temp files generated ny the library
     * unless it's otherwise instructed not to do
     * so by the preserveZipFile variable
     *
     * @param preserveZipFile bool
     */
    public void clearTempFiles(boolean preserveZipFile) {

        //delete the temp sql file
        File sqlFile = new File(dirName + "/sql/" + sqlFileName);
        if (sqlFile.exists()) {
            boolean res = sqlFile.delete();
            logger.debug(LOG_PREFIX + ": " + sqlFile.getAbsolutePath() + " deleted successfully? " + (res ? " TRUE " : " FALSE "));
        } else {
            logger.debug(LOG_PREFIX + ": " + sqlFile.getAbsolutePath() + " DOES NOT EXIST while clearing Temp Files");
        }

        File sqlFolder = new File(dirName + "/sql");
        if (sqlFolder.exists()) {
            boolean res = sqlFolder.delete();
            logger.debug(LOG_PREFIX + ": " + sqlFolder.getAbsolutePath() + " deleted successfully? " + (res ? " TRUE " : " FALSE "));
        } else {
            logger.debug(LOG_PREFIX + ": " + sqlFolder.getAbsolutePath() + " DOES NOT EXIST while clearing Temp Files");
        }


        //only execute this section if the
        //file is not to be preserved

        if (!preserveZipFile) {

            //delete the zipFile
            File zipFile = new File(zipFileName);
            if (zipFile.exists()) {
                boolean res = zipFile.delete();
                logger.debug(LOG_PREFIX + ": " + zipFile.getAbsolutePath() + " deleted successfully? " + (res ? " TRUE " : " FALSE "));
            } else {
                logger.debug(LOG_PREFIX + ": " + zipFile.getAbsolutePath() + " DOES NOT EXIST while clearing Temp Files");
            }

            //delete the temp folder
            File folder = new File(dirName);
            if (folder.exists()) {
                boolean res = folder.delete();
                logger.debug(LOG_PREFIX + ": " + folder.getAbsolutePath() + " deleted successfully? " + (res ? " TRUE " : " FALSE "));
            } else {
                logger.debug(LOG_PREFIX + ": " + folder.getAbsolutePath() + " DOES NOT EXIST while clearing Temp Files");
            }
        }

        logger.debug(LOG_PREFIX + ": generated temp files cleared successfully");
    }

    /**
     * This will get the final output
     * sql file name.
     *
     * @return String
     */
    public String getSqlFilename() {
        return isSqlFileNamePropertySet() ? properties.getProperty(SQL_FILE_NAME) + ".sql" :
                new SimpleDateFormat("d_M_Y_H_mm_ss").format(new Date()) + "_" + database + "_database_dump.sql";
    }

    public String getSqlFileName() {
        return sqlFileName;
    }

    public String getGeneratedSql() {
        return generatedSql;
    }

    public File getGeneratedZipFile() {
        if (generatedZipFile != null && generatedZipFile.exists()) {
            return generatedZipFile;
        }
        return null;
    }
}
