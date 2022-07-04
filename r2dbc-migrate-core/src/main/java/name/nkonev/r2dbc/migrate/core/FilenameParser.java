package name.nkonev.r2dbc.migrate.core;

import java.util.Arrays;
import java.util.List;

public abstract class FilenameParser {

    public static class MigrationInfo {
        private int version;
        private String description;
        private boolean splitByLine;
        private boolean transactional;

        private boolean premigration;

        private boolean postmigration;

        public MigrationInfo(int version, String description, boolean splitByLine, boolean transactional, boolean premigration, boolean postmigration) {
            this.version = version;
            this.description = description;
            this.splitByLine = splitByLine;
            this.transactional = transactional;
            this.premigration = premigration;
            this.postmigration = postmigration;
        }

        public String getDescription() {
            return description;
        }

        public int getVersion() {
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

        public boolean isPostmigration() {
            return postmigration;
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
            boolean postmigration = modifiers.contains("postmigration");
            boolean split = modifiers.contains("split");
            return new MigrationInfo(getVersion(array[0]), getDescription(array[1]), split, !nonTransactional, premigration, postmigration);
        } else if (array.length == 2) {
            // no split
            return new MigrationInfo(getVersion(array[0]), getDescription(array[1]), false, true, false, false);
        } else {
            throw new RuntimeException("Invalid file name '" + filename + "'");
        }
    }

    private static int getVersion(String vPart) {
        String v = vPart.replace("V", "");
        return Integer.parseInt(v);
    }

    private static String getDescription(String descriptionPart) {
        return descriptionPart.replace("_", " ");
    }
}
