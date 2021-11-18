package com.grpc;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.retry.annotation.EnableRetry;

@SpringBootApplication
@EnableRetry
public class MasterGrpcApplication {

  public static void main(String[] args) {
    SpringApplication.run(MasterGrpcApplication.class, args);
  }

}
