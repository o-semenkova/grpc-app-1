package com.grpc;

import java.util.NavigableSet;

import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public class GetCurrentMessagesKeys extends GetCurrentMasterMessageKeysServiceGrpc.GetCurrentMasterMessageKeysServiceImplBase {

  private SendMessageServiceImpl sendMessageService;

  public GetCurrentMessagesKeys(SendMessageServiceImpl sendMessageService) {
    this.sendMessageService = sendMessageService;
  }

  public void getCurrentMessageKeys(EmptyParams emptyParams, StreamObserver<MessageKey> responseObserver) {
    NavigableSet<Long> masterKeys = sendMessageService.getCurrentMessagesKeys();
    MessageKey keys = MessageKey.newBuilder().addAllId(masterKeys).build();
    responseObserver.onNext(keys);
    responseObserver.onCompleted();
  }
}
