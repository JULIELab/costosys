package de.julielab.xmlData.dataBase.util;

public class NoReservedConnectionException extends CoStoSysRuntimeException {
    public NoReservedConnectionException() {
    }

    public NoReservedConnectionException(String message) {
        super(message);
    }

    public NoReservedConnectionException(String message, Throwable cause) {
        super(message, cause);
    }

    public NoReservedConnectionException(Throwable cause) {
        super(cause);
    }

    public NoReservedConnectionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
