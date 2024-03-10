package name.nkonev.r2dbc.migrate.core;

public enum BunchOfResourcesType {
    /**
     * Files named according to convention, e. g. V1__create_customers.sql
     */
    CONVENTIONALLY_NAMED_FILES,

    /**
     * Arbitrary named files, their versions will be created according their order.
     * This option should be used carefully along with CONVENTIONALLY_NAMED_FILES due to version auto generation.
     */
    JUST_FILE
}
