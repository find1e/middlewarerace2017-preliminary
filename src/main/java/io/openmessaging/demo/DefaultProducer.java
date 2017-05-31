
package io.openmessaging.demo;
import io.openmessaging.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultProducer implements Producer {
    private MessageFactory messageFactory = new DefaultMessageFactory();
    private MessageStore messageStore = MessageStore.getInstance();
    private KeyValue properties;
    private ByteBuffer byteBuffer = ByteBuffer.allocate(SendConstants.buffSize);


    DefaultBytesMessage defaultBytesMessage = null;


    public DefaultProducer(KeyValue properties) {
        this.properties = properties;

    }






    @Override public synchronized BytesMessage createBytesMessageToTopic(String topic, byte[] body) {





        if (topic.substring(0, topic.indexOf("_")).equals("TOPIC")) {
            defaultBytesMessage = (DefaultBytesMessage) messageFactory.createBytesMessageToTopic(topic, body);
        } else {


            defaultBytesMessage = (DefaultBytesMessage) messageFactory.createBytesMessageToQueue(topic, body);


        }
        for (String  key :properties.keySet()) {
            defaultBytesMessage.putProperties(key, properties.getString(key));
        }
        return defaultBytesMessage;

    }

    @Override public synchronized BytesMessage createBytesMessageToQueue(String queue, byte[] body) {

        DefaultBytesMessage defaultBytesMessage = null;
       if (queue.substring(0, queue.indexOf("_")).equals("TOPIC")) {
           defaultBytesMessage = (DefaultBytesMessage) messageFactory.createBytesMessageToTopic(queue, body);
        } else {


           defaultBytesMessage = (DefaultBytesMessage) messageFactory.createBytesMessageToQueue(queue, body);


        }



        for (String  key :properties.keySet()) {
            defaultBytesMessage.putProperties(key, properties.getString(key));
        } return defaultBytesMessage;
    }


    @Override public KeyValue properties() {
        return properties;
    }

    @Override public void send(Message defaultBytesMessag) {



        //
//DefaultBytesMessage defaultBytesMessage = (DefaultBytesMessage) defaultBytesMessag;
//        for (String s : defaultBytesMessage.headers().keySet()) {
//            System.out.println(s+defaultBytesMessage.headers().getString(s));
//        }
//
//        for (String s : defaultBytesMessage.properties().keySet()) {
//            System.out.println(s+defaultBytesMessage.properties().getString(s));
//        }
//        String body = new String(defaultBytesMessage.getBody());
//
//        System.out.println(new String(body));
//
//        System.out.println("--------------------------------");

            messageStore.putMessage((DefaultBytesMessage) defaultBytesMessag,properties,this);


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

        messageStore.flush(properties,byteBuffer);
    }

    @Override
    public void start() {

    }

    @Override
    public void shutdown() {

    }
    public ByteBuffer getByteBuffer(){
        return byteBuffer;
    }
    public void setByteBuffer(ByteBuffer byteBuffer){
        this.byteBuffer = byteBuffer;

    }
}
