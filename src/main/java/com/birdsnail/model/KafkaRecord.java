package com.birdsnail.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * kafka发送的消息对象
 *
 * @author BirdSnail
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class KafkaRecord implements Serializable {
    private int id;
    private String message;
    private long timeStamp;
}