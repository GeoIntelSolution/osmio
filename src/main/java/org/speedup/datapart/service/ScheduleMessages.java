package org.speedup.datapart.service;

import com.github.javafaker.ChuckNorris;
import com.github.javafaker.Faker;
import org.apache.logging.log4j.message.SimpleMessage;
import org.speedup.datapart.controller.GreetingController;
import org.speedup.datapart.vo.OutputMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.converter.SimpleMessageConverter;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.Date;

@Service
public class ScheduleMessages {
    private final SimpMessagingTemplate simpMessagingTemplate;

    private final Faker faker;


    public ScheduleMessages(SimpMessagingTemplate simpMessagingTemplate) {
        this.simpMessagingTemplate = simpMessagingTemplate;
        faker = new Faker();
    }
    @Scheduled(fixedRate = 5000)
    public void sendMessage(){
        final String time = new SimpleDateFormat("HH:mm").format(new Date());
        simpMessagingTemplate.convertAndSend("/topic/pushmessages",
                new OutputMessage("Chuck Norris", faker.chuckNorris().fact(), time));
    }

}


