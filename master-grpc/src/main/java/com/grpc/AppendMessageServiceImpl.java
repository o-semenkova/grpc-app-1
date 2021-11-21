package com.grpc;

import java.util.concurrent.TimeUnit;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

@Service
public class AppendMessageServiceImpl {

@Retryable(value = RuntimeException.class, maxAttempts = 200000, backoff = @Backoff(delay = 100))
  public LogMessageAck append(LogMessage msg, String host, int port) {
    return appendMsg(host, port, msg);
  }
  @Retryable(value = RuntimeException.class, maxAttempts = 200000, backoff = @Backoff(delay = 100))
  private LogMessageAck appendMsg(String host, int port, LogMessage msg) {
    ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                                                  .usePlaintext()
                                                  .build();

    AppendMessageServiceGrpc.AppendMessageServiceBlockingStub stub = AppendMessageServiceGrpc.newBlockingStub(channel);
    LogMessageAck ack = stub.append(msg);
    try {
      channel.shutdown().awaitTermination(600, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return ack;
  }
}
