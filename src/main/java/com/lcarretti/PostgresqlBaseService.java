package com.lcarretti;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Created by lcarretti on 30-Sep-19.
 */
public class PostgresqlBaseService {

    private static Logger logger = LoggerFactory.getLogger(PostgresqlBaseService.class);

    static final String SQL_START_PATTERN = "-- start";
    static final String SQL_END_PATTERN = "-- end";

    /**
     * This is a utility function for connecting to a
     * database instance that's running on localhost at port 5432.
     * It will build a JDBC URL from the given parameters and use that to
     * obtain a connect from doConnect()
     *
     * @param username   database username
     * @param password   database password
     * @param database   database name
     * @param driverName the user supplied mysql connector driver class name. Can be empty
     * @return Connection
     * @throws ClassNotFoundException exception
     * @throws SQLException           exception
     */
    static Connection connect(String username, String password, String database, String driverName) throws ClassNotFoundException, SQLException {
        String url = "jdbc:postgresql://localhost:5432/" + database;
        String driver = (Objects.isNull(driverName) || driverName.isEmpty()) ? "org.postgresql.Driver" : driverName;
        return doConnect(driver, url, username, password);
    }

    /**
     * This is a utility function that allows connecting
     * to a database instance identified by the provided jdbcURL
     * The connector driver name can be empty
     *
     * @param username   database username
     * @param password   database password
     * @param jdbcURL    the user supplied JDBC URL. It's used as is. So ensure you supply the right parameters
     * @param driverName the user supplied mysql connector driver class name
     * @return Connection
     * @throws ClassNotFoundException exception
     * @throws SQLException           exception
     */
    static Connection connectWithURL(String username, String password, String jdbcURL, String driverName) throws ClassNotFoundException, SQLException {
        String driver = (Objects.isNull(driverName) || driverName.isEmpty()) ? "org.postgresql.Driver" : driverName;
        return doConnect(driver, jdbcURL, username, password);
    }

    /**
     * This will attempt to connect to a database using
     * the provided parameters.
     * On success it'll return the java.sql.Connection object
     *
     * @param driver   the class name for the mysql driver to use
     * @param url      the url of the database
     * @param username database username
     * @param password database password
     * @return Connection
     * @throws SQLException           exception
     * @throws ClassNotFoundException exception
     */
    private static Connection doConnect(String driver, String url, String username, String password) throws SQLException, ClassNotFoundException {
        Class.forName(driver);
        Connection connection = DriverManager.getConnection(url, username, password);
        logger.debug("DB Connected Successfully");
        return connection;
    }


    /**
     * This is a utility function to get the names of all
     * the tables that're in the database supplied
     *
     * @param stmt Statement object
     * @return List\<String\>
     * @throws SQLException exception
     */
    static List<String> getAllTables(Statement stmt) throws SQLException {
        List<String> table = new ArrayList<>();
        ResultSet rs;
        rs = stmt.executeQuery("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';");
        while (rs.next()) {
            table.add(rs.getString("tablename"));
        }
        return table;
    }

    /**
     * Create a function to create the similar function of MySQL 'SHOW CREATE TABLE'
     * @param stmt Statement object
     * @throws SQLException exception
     */
    static void createPostgresSqlFunction(Statement stmt) throws SQLException {
        stmt.execute("CREATE OR REPLACE FUNCTION public.generate_create_table_statement(p_table_name CHARACTER varying) RETURNS\n" +
                "SETOF text AS $BODY$\n" +
                "DECLARE\n" +
                "    v_table_ddl   text;\n" +
                "    column_record record;\n" +
                "    table_rec record;\n" +
                "    constraint_rec record;\n" +
                "    firstrec boolean;\n" +
                "BEGIN\n" +
                "    FOR table_rec IN\n" +
                "        SELECT c.relname FROM pg_catalog.pg_class c\n" +
                "            LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n" +
                "                WHERE relkind = 'r'\n" +
                "                AND relname~ ('^('||p_table_name||')$')\n" +
                "                AND n.nspname <> 'pg_catalog'\n" +
                "                AND n.nspname <> 'information_schema'\n" +
                "                AND n.nspname !~ '^pg_toast'\n" +
                "                AND pg_catalog.pg_table_is_visible(c.oid)\n" +
                "          ORDER BY c.relname\n" +
                "    LOOP\n" +
                "\n" +
                "        FOR column_record IN\n" +
                "            SELECT\n" +
                "                b.nspname as schema_name,\n" +
                "                b.relname as table_name,\n" +
                "                a.attname as column_name,\n" +
                "                pg_catalog.format_type(a.atttypid, a.atttypmod) as column_type,\n" +
                "                CASE WHEN\n" +
                "                    (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)\n" +
                "                     FROM pg_catalog.pg_attrdef d\n" +
                "                     WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef) IS NOT NULL THEN\n" +
                "                    'DEFAULT '|| (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)\n" +
                "                                  FROM pg_catalog.pg_attrdef d\n" +
                "                                  WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef)\n" +
                "                ELSE\n" +
                "                    ''\n" +
                "                END as column_default_value,\n" +
                "                CASE WHEN a.attnotnull = true THEN\n" +
                "                    'NOT NULL'\n" +
                "                ELSE\n" +
                "                    'NULL'\n" +
                "                END as column_not_null,\n" +
                "                a.attnum as attnum,\n" +
                "                e.max_attnum as max_attnum\n" +
                "            FROM\n" +
                "                pg_catalog.pg_attribute a\n" +
                "                INNER JOIN\n" +
                "                 (SELECT c.oid,\n" +
                "                    n.nspname,\n" +
                "                    c.relname\n" +
                "                  FROM pg_catalog.pg_class c\n" +
                "                       LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n" +
                "                  WHERE c.relname = table_rec.relname\n" +
                "                    AND pg_catalog.pg_table_is_visible(c.oid)\n" +
                "                  ORDER BY 2, 3) b\n" +
                "                ON a.attrelid = b.oid\n" +
                "                INNER JOIN\n" +
                "                 (SELECT\n" +
                "                      a.attrelid,\n" +
                "                      max(a.attnum) as max_attnum\n" +
                "                  FROM pg_catalog.pg_attribute a\n" +
                "                  WHERE a.attnum > 0\n" +
                "                    AND NOT a.attisdropped\n" +
                "                  GROUP BY a.attrelid) e\n" +
                "                ON a.attrelid=e.attrelid\n" +
                "            WHERE a.attnum > 0\n" +
                "              AND NOT a.attisdropped\n" +
                "            ORDER BY a.attnum\n" +
                "        LOOP\n" +
                "            IF column_record.attnum = 1 THEN\n" +
                "                v_table_ddl:='CREATE TABLE '||column_record.schema_name||'.'||column_record.table_name||' (';\n" +
                "            ELSE\n" +
                "                v_table_ddl:=v_table_ddl||',';\n" +
                "            END IF;\n" +
                "\n" +
                "            IF column_record.attnum <= column_record.max_attnum THEN\n" +
                "                v_table_ddl:=v_table_ddl||chr(10)||\n" +
                "                         '    '||column_record.column_name||' '||column_record.column_type||' '||column_record.column_default_value||' '||column_record.column_not_null;\n" +
                "            END IF;\n" +
                "        END LOOP;\n" +
                "\n" +
                "        firstrec := TRUE;\n" +
                "        FOR constraint_rec IN\n" +
                "            SELECT conname, pg_get_constraintdef(c.oid) as constrainddef\n" +
                "                FROM pg_constraint c\n" +
                "                    WHERE conrelid=(\n" +
                "                        SELECT attrelid FROM pg_attribute\n" +
                "                        WHERE attrelid = (\n" +
                "                            SELECT oid FROM pg_class WHERE relname = table_rec.relname\n" +
                "                        ) AND attname='tableoid'\n" +
                "                    )\n" +
                "        LOOP\n" +
                "            v_table_ddl:=v_table_ddl||','||chr(10);\n" +
                "            v_table_ddl:=v_table_ddl||'CONSTRAINT '||constraint_rec.conname;\n" +
                "            v_table_ddl:=v_table_ddl||chr(10)||'    '||constraint_rec.constrainddef;\n" +
                "            firstrec := FALSE;\n" +
                "        END LOOP;\n" +
                "        v_table_ddl:=v_table_ddl||');';\n" +
                "        RETURN NEXT v_table_ddl;\n" +
                "    END LOOP;\n" +
                "END;\n" +
                "$BODY$ LANGUAGE PLPGSQL VOLATILE COST 100;");
    }

    /**
     * Delete created previous function at the end of export
     *
     * @param stmt Statement object
     * @throws SQLException exception
     */
    static void deletePostgresSqlFunction(Statement stmt) throws SQLException {
        stmt.execute("DROP FUNCTION generate_create_table_statement(p_table_name varchar);");
    }
}
