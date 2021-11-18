package com.grpc;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
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

  public List<Long> getCurrentMessageKeysFromMaster() {
    ManagedChannel channel = ManagedChannelBuilder.forAddress("master-grpc", 9090)
                                                  .usePlaintext()
                                                  .build();
    GetCurrentMasterMessageKeysServiceGrpc.GetCurrentMasterMessageKeysServiceBlockingStub stub = GetCurrentMasterMessageKeysServiceGrpc.newBlockingStub(channel);
    MessageKey messageKeys = stub.getCurrentMessageKeys(EmptyParams.getDefaultInstance());
    List<Long> keys = messageKeys.getIdList();
    // messageKeys.getIdList - unmodifiable collection - no need to sort()
//    Collections.sort(keys);
    channel.shutdownNow();
    return keys;
  }

  private String convertWithStream(Map<Long, String> map) {
    String mapAsString = map.keySet().stream()
                            .map(key -> map.get(key))
                            .collect(Collectors.joining(", ", "{", "}"));
    return mapAsString;
  }

  public String getAllMessages() {
    List<Long> masterKeys = getCurrentMessageKeysFromMaster();
    NavigableSet<Long> keys = messages.navigableKeySet();
    Map<Long, String> toPrint = new ConcurrentSkipListMap<>();
    for(Long key: masterKeys) {
      if(keys.contains(key)) {
        toPrint.put(key, messages.get(key));
      } else {
        break;
      }
    }
    return convertWithStream(toPrint);
  }
}
