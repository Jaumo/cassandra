package org.apache.cassandra.streaming;

public enum StreamOperation
{

    OTHER("Other"), // Fallback to avoid null types when deserializing from string
    RESTORE_REPLICA_COUNT("Restore replica count"), // Handles removeNode
    DECOMMISSION("Unbootstrap"),
    RELOCATION("Relocation"),
    BOOTSTRAP("Bootstrap"),
    REBUILD("Rebuild"),
    BULK_LOAD("Bulk Load"),
    REPAIR("Repair")
    ;

    private final String type;

    StreamOperation(final String type) {
        this.type = type;
    }

    public static StreamOperation fromString(String text) {
        for (StreamOperation b : StreamOperation.values()) {
            if (b.type.equalsIgnoreCase(text)) {
                return b;
            }
        }

        return OTHER;
    }

    public String getDescription() {
        return type;
    }
}
