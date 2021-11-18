package com.grpc;

import java.util.NavigableSet;
import java.util.Random;

import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

@Service
public class AppendMessageServiceImpl {

  // To emulate non-Runtime exception for Retry mechanism testing
  private static final Random random = new Random();
  private static final float UNAVAILABLE_PERCENTAGE = 0.5F;

  @Retryable(include = {RuntimeException.class, RetryException.class}, maxAttempts = 200000, backoff = @Backoff(delay = 100))
  public LogMessageAck append(LogMessage msg, String host, int port) {
    // To emulate non-Runtime exception for Retry mechanism testing
    if(random.nextFloat() < UNAVAILABLE_PERCENTAGE) {
      try {
        throw  new RetryException();
      } catch (RetryException e) {
        e.printStackTrace();
      }
    }
    return appendMsg(host, port, msg);
  }
  @Retryable(value = RuntimeException.class, maxAttempts = 200000, backoff = @Backoff(delay = 100))
  private LogMessageAck appendMsg(String host, int port, LogMessage msg) {
    ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                                                  .usePlaintext()
                                                  .build();

    AppendMessageServiceGrpc.AppendMessageServiceBlockingStub stub = AppendMessageServiceGrpc.newBlockingStub(channel);
    LogMessageAck ack = stub.append(msg);

    return ack;
  }
}
