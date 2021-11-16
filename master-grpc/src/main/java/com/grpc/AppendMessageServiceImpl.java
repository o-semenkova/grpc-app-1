package com.grpc;

import org.springframework.stereotype.Service;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

@Service
public class AppendMessageServiceImpl {

  public LogMessageAck append(LogMessage msg, String host, int port) {
    return appendMsg(host, port, msg);
  }

  private LogMessageAck appendMsg(String host, int port, LogMessage msg) {
    ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                                                  .usePlaintext()
                                                  .build();

    AppendMessageServiceGrpc.AppendMessageServiceBlockingStub stub = AppendMessageServiceGrpc.newBlockingStub(channel);
    LogMessageAck ack = stub.append(msg);

    return ack;
  }
}
