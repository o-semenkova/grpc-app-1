package com.gprc;

import org.springframework.stereotype.Service;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import com.grpc.LogMessage;
import com.grpc.SendMessageServiceGrpc;
import com.grpc.LogMessageAck;

@Service
public class SendMessageServiceImpl {
  public String send() {
    ManagedChannel channel = ManagedChannelBuilder.forAddress("master-grpc", 9090)
                                                  .usePlaintext()
                                                  .build();
    SendMessageServiceGrpc.SendMessageServiceBlockingStub stub = SendMessageServiceGrpc.newBlockingStub(channel);
    LogMessageAck sendResponse = stub.send(com.grpc.LogMessage.newBuilder()
                                                                 .setId(0)
                                                                 .setW(1)
                                                                 .build());
    channel.shutdownNow();
    return sendResponse.getStatus();
  }

  public void sendmsg(LogMessage message) {
    ManagedChannel channel;
    if(message.getW() == 1) {
      channel = ManagedChannelBuilder.forAddress("master-grpc", 9090)
                                                    .usePlaintext()
                                                    .build();
      SendMessageServiceGrpc.SendMessageServiceStub stub = SendMessageServiceGrpc.newStub(channel);
      stub.send(message, new LogMessageCallback());
      try {
        Thread.sleep(3000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    } else {
      channel = ManagedChannelBuilder.forAddress("master-grpc", 9090)
                                                    .usePlaintext()
                                                    .build();
      SendMessageServiceGrpc.SendMessageServiceBlockingStub stub = SendMessageServiceGrpc.newBlockingStub(channel);
      stub.send(message);
    }
    channel.shutdownNow();
  }
}
