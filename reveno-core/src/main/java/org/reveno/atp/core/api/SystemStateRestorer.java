package org.reveno.atp.core.api;

public interface SystemStateRestorer {

    /**
     * 恢复
     * @param fromVersion
     * @param repository
     * @return
     */
    SystemState restore(long fromVersion, TxRepository repository);


    class SystemState {
        private long lastTransactionId;

        public SystemState(long lastTransactionId) {
            this.lastTransactionId = lastTransactionId;
        }

        public long getLastTransactionId() {
            return lastTransactionId;
        }
    }

}
