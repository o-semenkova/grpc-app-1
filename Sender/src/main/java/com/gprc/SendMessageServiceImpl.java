package com.gprc;

import org.springframework.stereotype.Service;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import com.grpc.LogMessage;
import com.grpc.SendMessageServiceGrpc;

@Service
public class SendMessageServiceImpl {
  public void sendmsg(LogMessage message) {
    ManagedChannel channel = ManagedChannelBuilder.forAddress("master-grpc", 9090)
                                                    .usePlaintext()
                                                    .build();
      SendMessageServiceGrpc.SendMessageServiceBlockingStub stub = SendMessageServiceGrpc.newBlockingStub(channel);
      stub.send(message);
    channel.shutdownNow();
  }
}
