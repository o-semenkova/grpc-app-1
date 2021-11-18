package com.gprc;

import java.util.concurrent.TimeUnit;

import org.springframework.stereotype.Service;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import com.grpc.LogMessage;
import com.grpc.SendMessageServiceGrpc;
import com.grpc.LogMessageAck;

@Service
public class SendMessageServiceImpl {

  public void sendmsg(LogMessage message) {
    ManagedChannel channel = ManagedChannelBuilder.forAddress("master-grpc", 9090)
                                                  .usePlaintext()
                                                  .build();
    SendMessageServiceGrpc.SendMessageServiceBlockingStub stub = SendMessageServiceGrpc.newBlockingStub(channel);
    stub.send(message);
    try {
      channel.shutdown().awaitTermination(600, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
