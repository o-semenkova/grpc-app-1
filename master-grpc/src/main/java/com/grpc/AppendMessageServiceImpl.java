package com.grpc;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.stereotype.Service;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;

@Service
public class AppendMessageServiceImpl {

  public List<Future<LogMessageAck>> append(LogMessage msg) {
//public String append(LogMessage msg) {

    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch cl = new CountDownLatch(msg.getW() - 1);
    Future<LogMessageAck> r1 = appendMsg("secondary-grpc", 9093, msg, executor, cl);
    Future<LogMessageAck> r2 = appendMsg("secondary-grpc-second", 9094, msg, executor, cl);
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
    return Stream.of(r1, r2).collect(Collectors.toList());
//    return LogMessageAck.newBuilder().setStatus("OK").build().getStatus();
  }

  private Future<LogMessageAck> appendMsg(String host, int port, LogMessage msg,
                         ExecutorService executor, CountDownLatch cl) {
    ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                                                  .usePlaintext()
                                                  .build();

    AppendMessageServiceGrpc.AppendMessageServiceFutureStub stub = AppendMessageServiceGrpc.newFutureStub(channel);
    ListenableFuture<LogMessageAck> logMessageAckListenableFuture = stub.append(msg);

    executor.execute(() -> {
      try {
        LogMessageAck logMessageAck = logMessageAckListenableFuture.get();
      } catch (Exception e) {
        e.printStackTrace();
      }
      cl.countDown();
    });


    //    executor.execute(() -> {
//      stub1.append(msg);
//      cl.countDown();
//    });
//    return response;
    return logMessageAckListenableFuture;
  }
}
