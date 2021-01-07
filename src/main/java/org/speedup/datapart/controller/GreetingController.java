package org.speedup.datapart.controller;

import org.speedup.datapart.vo.*;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Controller;
import org.springframework.web.util.HtmlUtils;

import java.text.SimpleDateFormat;
import java.util.Date;

@Controller
public class GreetingController {
    private Status status;

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    @MessageMapping("/hello")
    @SendTo("/topic/greetings")
    public Greeting greeting(SocketMessage message) throws Exception{
        status=new Status(1);
        Thread.sleep(1000);
        status.setCode(2);
        return new Greeting("Hello"+ HtmlUtils.htmlEscape(message.getName())+"!");
    }

    @MessageMapping("/chatwithbots")
    @SendTo("/topic/pushmessages")
    public OutputMessage send(final Message message) throws Exception {

        final String time = new SimpleDateFormat("HH:mm").format(new Date());
        return new OutputMessage(message.getFrom(), message.getText(), time);
    }

    @MessageMapping("/chat")
    @SendTo("/topic/messages")
    public OutputMessage send2(final Message message) throws Exception {

        final String time = new SimpleDateFormat("HH:mm").format(new Date());
        return new OutputMessage(message.getFrom(), message.getText(), time);
    }



}
