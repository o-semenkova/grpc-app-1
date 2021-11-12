package com.grpc;

import org.checkerframework.checker.nullness.compatqual.NullableDecl;

import com.google.common.util.concurrent.FutureCallback;

import io.grpc.stub.StreamObserver;

//public class LogMessageCallback implements StreamObserver<LogMessageAck> {
//
//  @Override
//  public void onNext(LogMessageAck logMessageAck) {
//
//  }
//
//  @Override
//  public void onError(Throwable throwable) {
//
//  }
//
//  @Override
//  public void onCompleted() {
//
//  }
//}

public class LogMessageCallback implements FutureCallback<LogMessageAck> {

  @Override
  public void onSuccess(@NullableDecl LogMessageAck logMessageAck) {

  }

  @Override
  public void onFailure(Throwable throwable) {

  }
}
