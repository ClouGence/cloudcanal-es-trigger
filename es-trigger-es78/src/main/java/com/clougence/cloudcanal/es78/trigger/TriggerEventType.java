package com.clougence.cloudcanal.es78.trigger;

public enum TriggerEventType {

    /**
     * Insert DML type.
     */
    INSERT("I"),
    /**
     * Update DML type.
     */
    UPDATE("U"),
    /**
     * Delete DML type.
     */
    DELETE("D");

    private final String code;

    TriggerEventType(String code){
        this.code = code;
    }

    public boolean isDml() { return this == INSERT || this == DELETE || this == UPDATE; }

    public String getCode() { return this.code; }

    public static TriggerEventType getEventType(String s) {
        for (TriggerEventType e : values()) {
            if (e.getCode().equals(s)) {
                return e;
            }
        }
        throw new IllegalStateException(String.format("Invalid data event type of %s", s));
    }
}
