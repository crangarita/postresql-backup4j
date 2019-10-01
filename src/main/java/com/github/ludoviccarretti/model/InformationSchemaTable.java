package com.github.ludoviccarretti.model;

public class InformationSchemaTable implements InformationSchemaGenerator {
    private final String tableName;
    private final String sql;

    public InformationSchemaTable(String tableName, String sql) {
        this.tableName = tableName;
        this.sql = sql;
    }

    public String getTableName() {
        return tableName;
    }

    public String getSql() {
        return sql;
    }

    @Override
    public String toString() {
        return "InformationSchemaTable{" +
                "tableName='" + tableName + '\'' +
                '}';
    }

    @Override
    public String getName() {
        return this.tableName;
    }

    @Override
    public String toSQL() {
        return this.sql;
    }

    public static final class InformationSchemaTableBuilder {
        private String tableName;
        private String sql;

        private InformationSchemaTableBuilder() {
        }

        public static InformationSchemaTableBuilder anInformationSchemaTable() {
            return new InformationSchemaTableBuilder();
        }

        public InformationSchemaTableBuilder withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public InformationSchemaTableBuilder withSql(String sql) {
            this.sql = sql;
            return this;
        }

        public InformationSchemaTable build() {
            return new InformationSchemaTable(tableName, sql);
        }
    }
}
