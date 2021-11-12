package com.grpc;

import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public class SendMessageServiceImpl extends SendMessageServiceGrpc.SendMessageServiceImplBase {

  private ConcurrentNavigableMap<Long, Integer> messages = new ConcurrentSkipListMap<>();
  private AppendMessageServiceImpl appendMessageService;
  private Long counter = 1L;

  public SendMessageServiceImpl(AppendMessageServiceImpl appendMessageService) {
    this.appendMessageService = appendMessageService;
  }

  public void send(LogMessage request, StreamObserver<LogMessageAck> responseObserver) {
    Long internalId = counter++;
    LogMessage msg = LogMessage.newBuilder().setId(internalId).setW(request.getW()).build();
    messages.put(internalId, request.getW());
    LogMessageAck ack;
    if (msg.getW() == 1) {
      ack = LogMessageAck.newBuilder().setStatus("OK").build();
      responseObserver.onNext(ack);
      responseObserver.onCompleted();
      appendMessageService.append(msg);
    } else {
      String status = appendMessageService.append(msg);
      ack = LogMessageAck.newBuilder().setStatus(status).build();
      responseObserver.onNext(ack);
      responseObserver.onCompleted();
    }
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
