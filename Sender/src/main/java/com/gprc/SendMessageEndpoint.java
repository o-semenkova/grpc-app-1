package com.gprc;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.grpc.LogMessage;

@RestController
public class SendMessageEndpoint {
  SendMessageServiceImpl messageService;

  public SendMessageEndpoint(SendMessageServiceImpl messageService) {
    this.messageService = messageService;
  }

  @GetMapping("/send")
  public String send() {
    return messageService.send();
  }

  @PostMapping(value = "/sendMsg",
               consumes = MediaType.APPLICATION_JSON_VALUE,
               produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> send(@RequestBody LogMessage message){
    messageService.sendmsg(message);
    return ResponseEntity.ok().build();
  }
}
