package name.nkonev.r2dbc.migrate.core;

public enum BunchOfResourcesType {
    /**
     * Files named according to the convention, e.g. V1__create_customers.sql
     */
    CONVENTIONALLY_NAMED_FILES,

    /**
     * Arbitrary named file, its version should be declared in {@link name.nkonev.r2dbc.migrate.core.BunchOfResourcesEntry}.
     */
    JUST_FILE
}
