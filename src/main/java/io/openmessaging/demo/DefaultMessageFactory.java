
package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.MessageFactory;
import io.openmessaging.MessageHeader;

import java.io.File;
import java.security.Key;

public class DefaultMessageFactory implements MessageFactory {

    @Override public BytesMessage createBytesMessageToTopic(String bucket, byte[] body) {
        DefaultBytesMessage defaultBytesMessage = new DefaultBytesMessage(body);
//        String name=bucket.substring(0,bucket.indexOf("_"));
//        String Type="QUEUE".equals(name)?MessageHeader.QUEUE:MessageHeader.TOPIC;
        defaultBytesMessage.putHeaders(MessageHeader.TOPIC,bucket);
        return defaultBytesMessage;
    }


    @Override public BytesMessage createBytesMessageToQueue(String bucket, byte[] body) {
        DefaultBytesMessage defaultBytesMessage = new DefaultBytesMessage(body);
        /*String name=bucket.substring(0,bucket.indexOf("_"));
        String Type="QUEUE".equals(name)?MessageHeader.QUEUE:MessageHeader.TOPIC;*/


        defaultBytesMessage.putHeaders(MessageHeader.QUEUE,bucket);
        return defaultBytesMessage;
    }
}