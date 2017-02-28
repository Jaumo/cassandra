package org.apache.cassandra.streaming;

public enum StreamType
{

    TEST_TRANSFER("StreamingTransferTest"),
    TEST_LEGACY_STREAMING("LegacyStreamingTest"),
    RESTORE_REPLICA_COUNT("Restore replica count"),
    DECOMMISSION("Unbootstrap"),
    RELOCATION("Relocation"),
    BOOTSTRAP("Bootstrap"),
    REBUILD("Rebuild"),
    BULK_LOAD("Bulk Load"),
    REPAIR("Repair")
    ;

    private final String type;

    StreamType(final String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return type;
    }
}
