package com.grpc;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public class SendMessageServiceImpl extends SendMessageServiceGrpc.SendMessageServiceImplBase {

  private ConcurrentNavigableMap<Long, String> messages = new ConcurrentSkipListMap<>();
  private AppendMessageServiceImpl appendMessageService;
  private Long counter = 1L;

  public SendMessageServiceImpl(AppendMessageServiceImpl appendMessageService) {
    this.appendMessageService = appendMessageService;
  }

  public void send(LogMessage request, StreamObserver<LogMessageAck> responseObserver) {
    Long internalId = counter++;
    LogMessage msg = LogMessage.newBuilder()
                               .setId(internalId)
                               .setW(request.getW())
                               .setMsg(request.getMsg())
                               .build();
    messages.put(internalId, "id=" + internalId +" w=" + request.getW() + " msg=" + request.getMsg());
    LogMessageAck ack;
    if (msg.getW() == 1) {
      ack = LogMessageAck.newBuilder().setStatus("OK").build();
      responseObserver.onNext(ack);
      responseObserver.onCompleted();
      appendMessageService.append(msg);
    } else {
      List<Future<LogMessageAck>> futures = appendMessageService.append(msg);
      boolean responsesIsReady = false;
      while (!responsesIsReady) {
        if(msg.getW() == 3) {
          responsesIsReady = futures.stream().allMatch(Future::isDone);
        } else {
          responsesIsReady = futures.stream().anyMatch(Future::isDone);
        }
      }

//      boolean isSuccessful = false;
//      if(responsesIsReady) {
//        if(msg.getW() == 3) {
//          isSuccessful = futures.stream().allMatch(f -> isOK(f));
//        } else {
//          isSuccessful = futures.stream().anyMatch(f -> isOK(f));
//        }
//      }

      if(responsesIsReady) {
        ack = LogMessageAck.newBuilder().setStatus(Status.OK.toString()).build();
      } else {
        ack = LogMessageAck.newBuilder().setStatus(Status.DATA_LOSS.toString()).build();
      }

//      ack = LogMessageAck.newBuilder().setStatus(Status.OK.toString()).build();
      responseObserver.onNext(ack);
      responseObserver.onCompleted();
    }
  }

  private boolean isOK(Future<LogMessageAck> f){
    boolean isOK = false;
    try{
      isOK = f.get().getStatus().equals(Status.OK.toString());
    } catch(Exception e) {

    }
    return isOK;
  }

  private String convertWithStream(Map<Long, String> map) {
    String mapAsString = map.keySet().stream()
                            .map(key -> map.get(key))
                            .collect(Collectors.joining(", ", "{", "}"));
    return mapAsString;
  }

  public String getAllMessages() {
    return convertWithStream(messages);
  }
}
