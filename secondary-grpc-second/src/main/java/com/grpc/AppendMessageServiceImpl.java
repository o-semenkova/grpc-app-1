package com.grpc;

import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public class AppendMessageServiceImpl extends com.grpc.AppendMessageServiceGrpc.AppendMessageServiceImplBase {

  private ConcurrentNavigableMap<Long, String> messages = new ConcurrentSkipListMap<>();

  @Override
  public void append(com.grpc.LogMessage request,
                                                     StreamObserver<com.grpc.LogMessageAck> responseObserver) {
    try {
      Thread.sleep(15000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    messages.put(request.getId(),  "id=" + request.getId() + " w=" + request.getW() + " msg=" + request.getMsg());

    com.grpc.LogMessageAck ack = com.grpc.LogMessageAck.newBuilder()
                                                                     .setId(request.getId())
                                                                     .setStatus("OK")
                                                                     .build();
    responseObserver.onNext(ack);
    responseObserver.onCompleted();
  }

  private String convertWithStream(Map<Long, String> map) {
    String mapAsString = map.keySet().stream()
                            .map(key -> map.get(key))
                            .collect(Collectors.joining(", ", "{", "}"));
    return mapAsString;
  }

  public String getAllMessages() {
    NavigableSet<Long> keys = messages.navigableKeySet();
    long idsSize = keys.size();
    Map<Long, String> toPrint = new ConcurrentSkipListMap<>();
    for (long i = 1; i <= idsSize; i++) {
      if(keys.contains(i)) {
        toPrint.put(i, messages.get(i));
      } else {
        break;
      }
    }
    return convertWithStream(toPrint);
  }
}
