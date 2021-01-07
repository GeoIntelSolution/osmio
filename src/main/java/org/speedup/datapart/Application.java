package org.speedup.datapart;

import org.speedup.datapart.properties.DBProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class Application implements ApplicationRunner {

    @Autowired
    ServerProperties serverProperties;

    @Autowired
    DBProperties dbProperties;
    public static void main(String[] args) {
        SpringApplication.run(Application.class,args);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        System.out.println(serverProperties.getPort());
        System.out.println(dbProperties.getHost());
    }
}
