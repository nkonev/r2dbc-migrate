package name.nkonev.r2dbc.migrate.core;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public abstract class FilenameParser {

    private static final String V = "V";

    private static final String DECIMAL_POINT = ".";

    private static final String UNDERSCORE = "_";

    public static class MigrationInfo {
        private String version;
        private String description;
        private boolean splitByLine;
        private boolean transactional;
        private Double doubleVersion;

        public MigrationInfo(String version, String description, boolean splitByLine, boolean transactional) {
            this.version = version;
            this.description = description;
            this.splitByLine = splitByLine;
            this.transactional = transactional;
            this.doubleVersion = findDoubleVersion(version);
        }

        public String getDescription() {
            return description;
        }

        public String getVersion() {
            return version;
        }

        public Double getDoubleVersion() {
            return doubleVersion;
        }

        public boolean isSplitByLine() {
            return splitByLine;
        }


        public boolean isTransactional() {
            return transactional;
        }

        @Override
        public String toString() {
            return "MigrationInfo{" +
                    "version=" + version +
                    ", description='" + description + '\'' +
                    ", splitByLine=" + splitByLine +
                    ", transactional=" + transactional +
                    '}';
        }

    }

    public static MigrationInfo getMigrationInfo(String filename) throws RuntimeException
    {
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
            boolean split = modifiers.contains("split");
            return new MigrationInfo(getVersion(array[0]), getDescription(array[1]), split, !nonTransactional);
        } else if (array.length == 2) {
            // no split
            return new MigrationInfo(getVersion(array[0]), getDescription(array[1]), false, true);
        } else {
            throw new RuntimeException("Invalid file name '" + filename + "'");
        }
    }

    private static String getVersion(String vPart)
    {
        return Optional.of(vPart)
                .map(val -> val.substring(val.indexOf(V) + 1))
                .orElseThrow(() -> new RuntimeException(String.format("Invalid version number %s", vPart)));
    }

    public static Double findDoubleVersion(final String versionString) {
        final String version = versionString.replace(UNDERSCORE, DECIMAL_POINT);

        return Optional.of(version)
                .filter(val -> val.contains(DECIMAL_POINT))
                .filter(val -> val.replace(DECIMAL_POINT, "").length() - val.length() == -2)
                .map(val -> val.indexOf(DECIMAL_POINT, val.indexOf(DECIMAL_POINT) + 1))
                .map(val -> version.substring(0, val) + version.substring(val + 1))
                .map(Double::parseDouble)
                .orElseGet(() -> Double.parseDouble(version));
    }

    private static String getDescription(String descriptionPart) {
        return descriptionPart.replace("_", " ");
    }
}
