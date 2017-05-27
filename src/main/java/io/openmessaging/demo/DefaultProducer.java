
package io.openmessaging.demo;
import io.openmessaging.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class DefaultProducer implements Producer {
    private MessageFactory messageFactory = new DefaultMessageFactory();
    private MessageStore messageStore = MessageStore.getInstance();
    private KeyValue properties;
    private HashMap hashMap=new HashMap();

    public DefaultProducer(KeyValue properties) {
        this.properties = properties;

    }






    @Override public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {



        DefaultBytesMessage defaultBytesMessage= (DefaultBytesMessage) messageFactory.createBytesMessageToTopic(topic, body);
       /* String key= (String) properties.keySet().toArray()[1];

        defaultBytesMessage.putProperties(key,properties.getString(key));*/
        return defaultBytesMessage;

    }

    @Override public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {

        DefaultBytesMessage defaultBytesMessage = null;
       if (queue.substring(0, queue.indexOf("_")).equals("QUEUE")) {

            defaultBytesMessage = (DefaultBytesMessage) messageFactory.createBytesMessageToQueue(queue, body);
        } else {

            defaultBytesMessage = (DefaultBytesMessage) messageFactory.createBytesMessageToTopic(queue, body);


        }
/*
        String key= (String) properties.keySet().toArray()[1];

        defaultBytesMessage.putProperties(key,properties.getString(key));*/
        return defaultBytesMessage;
    }


    @Override public KeyValue properties() {
        return properties;
    }

    @Override public void send(Message message) {




            messageStore.putMessage((DefaultBytesMessage) message,properties);


    }

    @Override public void send(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public Promise<Void> sendAsync(Message message) {
        return null;
    }

    @Override
    public Promise<Void> sendAsync(Message message, KeyValue properties) {
        return null;
    }


    @Override public void sendOneway(Message message) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void sendOneway(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public BatchToPartition createBatchToPartition(String partitionName) {
        return null;
    }

    @Override
    public BatchToPartition createBatchToPartition(String partitionName, KeyValue properties) {
        return null;
    }

    @Override
    public void flush() {

        messageStore.flush(properties);
    }

    @Override
    public void start() {

    }

    @Override
    public void shutdown() {

    }
}
