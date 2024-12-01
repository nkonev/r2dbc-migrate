package name.nkonev.r2dbc.migrate.core;

public class MigrationMetadata {
    private final long version;
    private final String description;
    private final boolean splitByLine;
    private final boolean transactional;
    private final boolean premigration;
    private final boolean substitute;

    public MigrationMetadata(long version, String description, boolean splitByLine, boolean transactional, boolean premigration, boolean substitute) {
        this.version = version;
        this.description = description;
        this.splitByLine = splitByLine;
        this.transactional = transactional;
        this.premigration = premigration;
        this.substitute = substitute;
    }

    public String getDescription() {
        return description;
    }

    public long getVersion() {
        return version;
    }

    public boolean isSplitByLine() {
        return splitByLine;
    }

    public boolean isTransactional() {
        return transactional;
    }

    public boolean isPremigration() {
        return premigration;
    }

    public boolean isSubstitute() {
        return substitute;
    }

    @Override
    public String toString() {
        return "MigrationMetadata{" +
                "version=" + version +
                ", description='" + description + '\'' +
                ", splitByLine=" + splitByLine +
                ", transactional=" + transactional +
                ", premigration=" + premigration +
                ", substitute=" + substitute +
                '}';
    }
}
