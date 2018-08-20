package de.julielab.xmlData.dataBase.util;

public class CoStoSysRuntimeException extends RuntimeException {
    public CoStoSysRuntimeException() {
    }

    public CoStoSysRuntimeException(String message) {
        super(message);
    }

    public CoStoSysRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public CoStoSysRuntimeException(Throwable cause) {
        super(cause);
    }

    public CoStoSysRuntimeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
