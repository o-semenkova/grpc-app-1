package com.grpc;

import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
    messages.put(internalId, "id=" + internalId + " w=" + request.getW() + " msg=" + request.getMsg());

    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch cl = new CountDownLatch(msg.getW() - 1);

    executor.execute(() -> {

      LogMessageAck ack = appendMessageService.append(msg, "secondary-grpc", 9093);
      if (ack.getStatus().equals(Status.OK.getCode().toString())) {
                cl.countDown();
      }
    });

    executor.execute(() -> {

      LogMessageAck ack = appendMessageService.append(msg, "secondary-grpc-second", 9094);
      if (ack.getStatus().equals(Status.OK.getCode().toString())) {
                cl.countDown();
      }
    });

    try {
      cl.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    executor.shutdown();
    try {
      executor.awaitTermination(2, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    LogMessageAck ack = LogMessageAck.newBuilder().setStatus(Status.OK.toString()).build();
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
    return convertWithStream(messages);
  }
}
