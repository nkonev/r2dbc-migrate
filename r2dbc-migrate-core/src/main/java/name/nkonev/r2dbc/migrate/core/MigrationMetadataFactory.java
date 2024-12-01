package name.nkonev.r2dbc.migrate.core;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public abstract class MigrationMetadataFactory {

    public static MigrationMetadata getMigrationMetadata(String filename) {
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
            boolean substitute = modifiers.contains("substitute");
            return new MigrationMetadata(getVersion(array[0]), getDescription(array[1]), split, !nonTransactional, premigration, substitute);
        } else if (array.length == 2) {
            // no split
            return new MigrationMetadata(getVersion(array[0]), getDescription(array[1]), false, true, false, false);
        } else {
            throw new RuntimeException("Invalid file name '" + filename + "'");
        }
    }

    public static MigrationMetadata getMigrationMetadata(long version, String description, Boolean splitByLine, Boolean transactional, Boolean premigration, Boolean substitute) {
        return new MigrationMetadata(
            version,
            description,
            Optional.ofNullable(splitByLine).orElse(false),
            Optional.ofNullable(transactional).orElse(false),
            Optional.ofNullable(premigration).orElse(false),
            Optional.ofNullable(substitute).orElse(false)
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
