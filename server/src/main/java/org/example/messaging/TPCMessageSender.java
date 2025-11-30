package org.example.messaging;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.TPCPrepareMessage;

import java.util.concurrent.ExecutorService;

public class TPCMessageSender extends MessageSender {

    private static final Logger logger = LogManager.getLogger(TPCMessageSender.class);

    public TPCMessageSender(int nodeId, ExecutorService networkExecutor) {
        super(nodeId, networkExecutor);
    }

    public void sendPrepare(int targetNodeId, ServerMessage<TPCPrepareMessage> prepare) {
        logger.info("Sending TPC Prepare to node {} : {}", targetNodeId, prepare);
    }

}
