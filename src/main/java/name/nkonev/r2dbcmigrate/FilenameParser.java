package name.nkonev.r2dbcmigrate;


public abstract class FilenameParser {

    public static class MigrationInfo {
        private String description;
        private long version;
        private boolean splitByLine;

        public MigrationInfo(String description, long version, boolean splitByLine) {
            this.description = description;
            this.version = version;
            this.splitByLine = splitByLine;
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
    }

    public static MigrationInfo getMigrationInfo(String filename) {
        final String sql = ".sql";
        if (filename == null || !filename.endsWith(sql)) {
            throw new RuntimeException("File name should ends with " + sql);
        }
        String substring = filename.substring(0, filename.length() - sql.length());
        String[] array = substring.split("__");
        if (array.length == 3 && array[2].equals("split")) {
            // modifiers: split
            return new MigrationInfo(getDescription(array[1]), getVersion(array[0]), true);
        } else if (array.length == 2) {
            // no split
            return new MigrationInfo(getDescription(array[1]), getVersion(array[0]), false);
        } else {
            throw new RuntimeException("Invalid file name '" + filename + "'");
        }
    }

    private static long getVersion(String vPart) {
        String v = vPart.replace("V", "");
        return Long.parseLong(v);
    }

    private static String getDescription(String descriptionPart) {
        return descriptionPart.replace("_", " ");
    }
}
