package org.example.messaging;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ExecutorService;

public class PaxosMessageSender extends MessageSender {

    private static final Logger logger = LogManager.getLogger(PaxosMessageSender.class);

    public PaxosMessageSender(int nodeId, ExecutorService networkExecutor) {
        super(nodeId, networkExecutor);
    }

}
