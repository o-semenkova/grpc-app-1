package com.grpc;

import org.checkerframework.checker.nullness.compatqual.NullableDecl;

import com.google.common.util.concurrent.FutureCallback;

public class LogMessageCallback implements FutureCallback<LogMessageAck> {

  @Override
  public void onSuccess(@NullableDecl LogMessageAck logMessageAck) {

  }

  @Override
  public void onFailure(Throwable throwable) {

  }
}
