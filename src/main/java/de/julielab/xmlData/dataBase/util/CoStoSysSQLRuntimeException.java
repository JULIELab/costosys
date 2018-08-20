package de.julielab.xmlData.dataBase.util;

public class CoStoSysSQLRuntimeException extends CoStoSysRuntimeException {
    public CoStoSysSQLRuntimeException() {
    }

    public CoStoSysSQLRuntimeException(String message) {
        super(message);
    }

    public CoStoSysSQLRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public CoStoSysSQLRuntimeException(Throwable cause) {
        super(cause);
    }

    public CoStoSysSQLRuntimeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
