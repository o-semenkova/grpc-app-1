package com.grpc;

import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public class AppendMessageServiceImpl extends com.grpc.AppendMessageServiceGrpc.AppendMessageServiceImplBase {

  private ConcurrentNavigableMap<Long, Integer> messages = new ConcurrentSkipListMap<>();

  @Override
  public void append(com.grpc.LogMessage request,
                                                     StreamObserver<com.grpc.LogMessageAck> responseObserver) {
    try {
      Thread.sleep(20000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    messages.put(request.getId(), request.getW());

    com.grpc.LogMessageAck ack = com.grpc.LogMessageAck.newBuilder()
                                                                     .setId(request.getId())
                                                                     .setStatus("OK")
                                                                     .build();
    responseObserver.onNext(ack);
    responseObserver.onCompleted();
  }

  private String convertWithStream(Map<Long, Integer> map) {
    String mapAsString = map.keySet().stream()
                            .map(key -> key + "=" + map.get(key))
                            .collect(Collectors.joining(", ", "{", "}"));
    return mapAsString;
  }

  public String getAllMessages() {
    return convertWithStream(messages);
  }
}
