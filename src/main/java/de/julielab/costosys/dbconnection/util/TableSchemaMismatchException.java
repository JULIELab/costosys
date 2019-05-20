package de.julielab.costosys.dbconnection.util;

public class TableSchemaMismatchException extends CoStoSysException {
    public TableSchemaMismatchException() {
    }

    public TableSchemaMismatchException(String message) {
        super(message);
    }

    public TableSchemaMismatchException(String message, Throwable cause) {
        super(message, cause);
    }

    public TableSchemaMismatchException(Throwable cause) {
        super(cause);
    }

    public TableSchemaMismatchException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
