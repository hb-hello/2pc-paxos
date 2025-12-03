package org.example.messaging;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class ServerActivityInterceptor implements ServerInterceptor {

    private final static Logger logger = LogManager.getLogger(ServerActivityInterceptor.class);

    private final AtomicBoolean activeFlag = new AtomicBoolean(false);
    private static final List<String> ALLOWED_METHODS = Arrays.asList(
            "org.example.CLIService/SetActiveFlag",
            "org.example.CLIService/GetDB",
            "org.example.CLIService/GetStatus",
            "org.example.CLIService/GetLog",
            "org.example.CLIService/GetNewViews",
            "org.example.CLIService/Reset",
            "org.example.CLIService/Ping",
            "org.example.ClientService/Ping",
            "org.example.PaxosService/Ping",
            "org.example.TPCService/Ping"
    );

    public void setActiveFlag(boolean active) {
        activeFlag.set(active);
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {

        if (activeFlag.get()) {
            return next.startCall(call, headers);
        } else {
            // Get the full name of the incoming RPC method.
            String fullMethodName = call.getMethodDescriptor().getFullMethodName();

            // Only setActiveFLag method is allowed when server is inactive
            if (ALLOWED_METHODS.contains(fullMethodName)) {
                // For the allowed request, continue the call chain.
                return next.startCall(call, headers);
            } else {
                // For all other methods, close the call with a 'PERMISSION_DENIED' status.
                logger.info("Interceptor: Server is inactive. Blocking request for method: {}", fullMethodName);

                // Do not call `next.startCall()`. This prevents the request from being
                // processed by the service method, effectively emulating a "silent" server.
                // The caller will eventually time out with a `DEADLINE_EXCEEDED` error.
                return new ServerCall.Listener<ReqT>() {}; // Return a no-op listener
            }
        }
    }
}
