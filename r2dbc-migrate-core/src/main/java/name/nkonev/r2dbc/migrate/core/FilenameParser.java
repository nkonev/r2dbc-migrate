package name.nkonev.r2dbc.migrate.core;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public abstract class FilenameParser {

    public static class MigrationInfo {
        private long version;
        private String description;
        private boolean splitByLine;
        private boolean transactional;

        private boolean premigration;

        public MigrationInfo(long version, String description, boolean splitByLine, boolean transactional, boolean premigration) {
            this.version = version;
            this.description = description;
            this.splitByLine = splitByLine;
            this.transactional = transactional;
            this.premigration = premigration;
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

        @Override
        public String toString() {
            return "MigrationInfo{" +
                "version=" + version +
                ", description='" + description + '\'' +
                ", splitByLine=" + splitByLine +
                ", transactional=" + transactional +
                ", premigration=" + premigration +
                '}';
        }
    }

    public static MigrationInfo getMigrationInfo(String filename) {
        final String sql = ".sql";
        if (filename == null || !filename.endsWith(sql)) {
            throw new RuntimeException("File name should ends with " + sql);
        }
        String substring = filename.substring(0, filename.length() - sql.length());
        String[] array = substring.split("__");
        if (array.length == 3) {
            String modifiersRaw = array[2];
            List<String> modifiers = Arrays.asList(modifiersRaw.split(","));
            boolean nonTransactional = modifiers.contains("nontransactional");
            boolean premigration = modifiers.contains("premigration");
            boolean split = modifiers.contains("split");
            return new MigrationInfo(getVersion(array[0]), getDescription(array[1]), split, !nonTransactional, premigration);
        } else if (array.length == 2) {
            // no split
            return new MigrationInfo(getVersion(array[0]), getDescription(array[1]), false, true, false);
        } else {
            throw new RuntimeException("Invalid file name '" + filename + "'");
        }
    }

    public static MigrationInfo getMigrationInfo(long version, String description, Boolean splitByLine, Boolean transactional, Boolean premigration) {
        return new MigrationInfo(
            version,
            description,
            Optional.ofNullable(splitByLine).orElse(false),
            Optional.ofNullable(transactional).orElse(false),
            Optional.ofNullable(premigration).orElse(false)
        );
    }

    private static long getVersion(String vPart) {
        String v = vPart.replace("V", "");
        return Long.parseLong(v);
    }

    private static String getDescription(String descriptionPart) {
        return descriptionPart.replace("_", " ");
    }
}
