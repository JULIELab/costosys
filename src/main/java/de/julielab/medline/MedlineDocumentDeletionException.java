package de.julielab.medline;

public class MedlineDocumentDeletionException extends MedlineUpdateException {
    public MedlineDocumentDeletionException() {
    }

    public MedlineDocumentDeletionException(String message) {
        super(message);
    }

    public MedlineDocumentDeletionException(String message, Throwable cause) {
        super(message, cause);
    }

    public MedlineDocumentDeletionException(Throwable cause) {
        super(cause);
    }

    public MedlineDocumentDeletionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
