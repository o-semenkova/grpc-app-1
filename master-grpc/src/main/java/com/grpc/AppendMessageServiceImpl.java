package com.grpc;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.stereotype.Service;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

@Service
public class AppendMessageServiceImpl {

  public String append(LogMessage msg) {

    ExecutorService executor = Executors.newFixedThreadPool(2);
    ListenableFuture<LogMessageAck> f1 = appendMsg("secondary-grpc", 9093, msg, 1000, executor);
    ListenableFuture<LogMessageAck> f2 = appendMsg("secondary-grpc-second", 9094, msg, 1000, executor);

    executor.shutdown();
    try {
      executor.awaitTermination(2, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    try {
      if(msg.getW() == 3) {
        if(f1.get(150000, TimeUnit.MILLISECONDS) != null && f2.get(150000, TimeUnit.MILLISECONDS) != null){
          return LogMessageAck.newBuilder().setStatus("OK").build().getStatus();
        }
      } else if(msg.getW() == 2) {
        if(f1.get(150000, TimeUnit.MILLISECONDS) != null || f2.get(1000, TimeUnit.MILLISECONDS) != null){
          return LogMessageAck.newBuilder().setStatus("OK").build().getStatus();
        }
      } else {
        return LogMessageAck.newBuilder().setStatus("OK").build().getStatus();
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      e.printStackTrace();
    }
    return null;
  }

  private ListenableFuture<LogMessageAck> appendMsg(String host, int port, LogMessage msg, int sleep, Executor executor) {
    ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                                                    .usePlaintext()
                                                    .build();
    AppendMessageServiceGrpc.AppendMessageServiceFutureStub stub = AppendMessageServiceGrpc.newFutureStub(channel);
    ListenableFuture<LogMessageAck> listenableFuture = stub.append(msg);
    Futures.addCallback(listenableFuture, new LogMessageCallback(), executor);

    try {
      Thread.sleep(sleep);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    channel.shutdown();
    try {
      channel.awaitTermination(5000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ex) {
      // TODO add logic
    }
    return listenableFuture;
  }
}
