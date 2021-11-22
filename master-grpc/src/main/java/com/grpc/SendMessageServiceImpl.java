package com.grpc;

import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public class SendMessageServiceImpl extends SendMessageServiceGrpc.SendMessageServiceImplBase {

  private ConcurrentNavigableMap<Long, String> messages = new ConcurrentSkipListMap<>();
  private AppendMessageServiceImpl appendMessageService;
  private AtomicLong counter = new AtomicLong(0);
  Logger logger = LoggerFactory.getLogger(SendMessageServiceImpl.class);

  public SendMessageServiceImpl(AppendMessageServiceImpl appendMessageService) {
    this.appendMessageService = appendMessageService;
  }

  public void send(LogMessage request, StreamObserver<LogMessageAck> responseObserver) {
    Long internalId = counter.addAndGet(1);
    LogMessage msg = LogMessage.newBuilder()
                               .setId(internalId)
                               .setW(request.getW())
                               .setMsg(request.getMsg())
                               .build();
    messages.put(internalId, "id=" + internalId + " w=" + request.getW() + " msg=" + request.getMsg());
    logger.info("Message: id="+ internalId + " w=" + request.getW() + " text=" + request.getMsg() + " was stored to Master.");

    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch cl = new CountDownLatch(msg.getW() - 1);

    executor.execute(() -> {

      LogMessageAck ack = appendMessageService.append(msg, "secondary-grpc", 9093);
      if (ack.getStatus().equals(Status.OK.getCode().toString())) {
        logger.info("Message: id="+ internalId + " w=" + request.getMsg() + " text=" + request.getMsg() + " was stored to Secondary 1.");
                cl.countDown();
      }
    });

    executor.execute(() -> {

      LogMessageAck ack = appendMessageService.append(msg, "secondary-grpc-second", 9094);
      if (ack.getStatus().equals(Status.OK.getCode().toString())) {
        logger.info("Message: id="+ internalId + " w=" + request.getMsg() + " text=" + request.getMsg() + " was stored to Secondary 2.");
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
