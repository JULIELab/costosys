package de.julielab.costosys.medline;

public class ConfigurationConstants {
    @Deprecated
    public static final String INSERTION_INPUT = "insertion.directory";
    @Deprecated
    public static final String UPDATE_INPUT = "update.directory";
    public static final String DIRECTORY = "directories.directory";
    public static final String DELETION = "documentdeletions.deletion";
    public static final String DELETER = "deleter";

    // Constants for sub configurations with #DELETION as root.
    public static final String TABLE = "tables.table";

    private ConfigurationConstants() {
    }
}
