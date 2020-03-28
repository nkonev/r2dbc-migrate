package name.nkonev.r2dbcmigrate;


public abstract class FilenameParser {

    public static class MigrationInfo {
        private int version;
        private String description;
        private boolean splitByLine;

        public MigrationInfo(int version, String description, boolean splitByLine) {
            this.version = version;
            this.description = description;
            this.splitByLine = splitByLine;
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

        @Override
        public String toString() {
            return "MigrationInfo{" +
                    "version=" + version +
                    ", description='" + description + '\'' +
                    ", splitByLine=" + splitByLine +
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
        if (array.length == 3 && array[2].equals("split")) {
            // modifiers: split
            return new MigrationInfo(getVersion(array[0]), getDescription(array[1]), true);
        } else if (array.length == 2) {
            // no split
            return new MigrationInfo(getVersion(array[0]), getDescription(array[1]), false);
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
