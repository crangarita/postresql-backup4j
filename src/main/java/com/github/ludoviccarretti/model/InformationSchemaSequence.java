package com.github.ludoviccarretti.model;

public class InformationSchemaSequence implements InformationSchemaGenerator {
    private final String sequenceSchema;
    private final String sequenceName;
    private final Long startValue;
    private final Long minimumValue;
    private final Long maximumValue;
    private final Long increment;
    private final boolean hasCycle;

    private InformationSchemaSequence(String sequenceSchema, String sequenceName, Long startValue, Long minimumValue, Long maximumValue, Long increment, boolean hasCycle) {
        this.sequenceSchema = sequenceSchema;
        this.sequenceName = sequenceName;
        this.startValue = startValue;
        this.minimumValue = minimumValue;
        this.maximumValue = maximumValue;
        this.increment = increment;
        this.hasCycle = hasCycle;
    }

    public String getSequenceSchema() {
        return sequenceSchema;
    }

    public String getSequenceName() {
        return sequenceName;
    }

    public Long getStartValue() {
        return startValue;
    }

    public Long getMinimumValue() {
        return minimumValue;
    }

    public Long getMaximumValue() {
        return maximumValue;
    }

    public Long getIncrement() {
        return increment;
    }

    public boolean isHasCycle() {
        return hasCycle;
    }

    @Override
    public String toString() {
        return "InformationSchemaSequence{" +
                "sequenceSchema='" + sequenceSchema + '\'' +
                ", sequenceName='" + sequenceName + '\'' +
                ", startValue=" + startValue +
                ", minimumValue=" + minimumValue +
                ", maximumValue=" + maximumValue +
                ", increment=" + increment +
                ", hasCycle=" + hasCycle +
                '}';
    }

    @Override
    public String getName() {
        return this.sequenceName;
    }

    @Override
    public String toSQL() {
        String sql = "CREATE SEQUENCE " + this.sequenceSchema + "." + this.sequenceName +
                " INCREMENT BY " + this.increment + " MINVALUE " + this.minimumValue +
                " MAXVALUE " + this.maximumValue + " START WITH " + this.startValue;

        return (this.hasCycle) ? sql + " CYCLE;" : sql + " NO CYCLE;";
    }


    public static final class InformationSchemaSequenceBuilder {
        private String sequenceSchema;
        private String sequenceName;
        private Long startValue;
        private Long minimumValue;
        private Long maximumValue;
        private Long increment;
        private boolean hasCycle;

        private InformationSchemaSequenceBuilder() {
        }

        public static InformationSchemaSequenceBuilder anInformationSchemaSequence() {
            return new InformationSchemaSequenceBuilder();
        }

        public InformationSchemaSequenceBuilder withSequenceSchema(String sequenceSchema) {
            this.sequenceSchema = sequenceSchema;
            return this;
        }

        public InformationSchemaSequenceBuilder withSequenceName(String sequenceName) {
            this.sequenceName = sequenceName;
            return this;
        }

        public InformationSchemaSequenceBuilder withStartValue(Long startValue) {
            this.startValue = startValue;
            return this;
        }

        public InformationSchemaSequenceBuilder withMinimumValue(Long minimumValue) {
            this.minimumValue = minimumValue;
            return this;
        }

        public InformationSchemaSequenceBuilder withMaximumValue(Long maximumValue) {
            this.maximumValue = maximumValue;
            return this;
        }

        public InformationSchemaSequenceBuilder withIncrement(Long increment) {
            this.increment = increment;
            return this;
        }

        public InformationSchemaSequenceBuilder withHasCycle(boolean hasCycle) {
            this.hasCycle = hasCycle;
            return this;
        }

        public InformationSchemaSequence build() {
            return new InformationSchemaSequence(sequenceSchema, sequenceName, startValue, minimumValue, maximumValue, increment, hasCycle);
        }
    }
}
