package de.julielab.costosys.medline;

public class DocumentDeletionException extends MedlineUpdateException {
    public DocumentDeletionException() {
    }

    public DocumentDeletionException(String message) {
        super(message);
    }

    public DocumentDeletionException(String message, Throwable cause) {
        super(message, cause);
    }

    public DocumentDeletionException(Throwable cause) {
        super(cause);
    }

    public DocumentDeletionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
