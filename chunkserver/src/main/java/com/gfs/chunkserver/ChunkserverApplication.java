package com.gfs.chunkserver;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class ChunkserverApplication {

    @Value("${chunkserver.id}")
    private String chunkServerId;

    @Value("${server.port}")
    private int port;

    @PostConstruct
    public void printConfig() {
        System.out.println("====================================");
        System.out.println("ChunkServer ID: " + chunkServerId);
        System.out.println("Server Port: " + port);
        System.out.println("====================================");
    }

    public static void main(String[] args) {
        SpringApplication.run(ChunkserverApplication.class, args);
    }

}
