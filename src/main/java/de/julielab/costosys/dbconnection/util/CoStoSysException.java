package de.julielab.costosys.dbconnection.util;

public class CoStoSysException extends Exception {
    public CoStoSysException() {
    }

    public CoStoSysException(String message) {
        super(message);
    }

    public CoStoSysException(String message, Throwable cause) {
        super(message, cause);
    }

    public CoStoSysException(Throwable cause) {
        super(cause);
    }

    public CoStoSysException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
