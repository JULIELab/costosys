package de.julielab.medline;

public class MedlineUpdateException extends Exception {
    public MedlineUpdateException() {
    }

    public MedlineUpdateException(String message) {
        super(message);
    }

    public MedlineUpdateException(String message, Throwable cause) {
        super(message, cause);
    }

    public MedlineUpdateException(Throwable cause) {
        super(cause);
    }

    public MedlineUpdateException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
