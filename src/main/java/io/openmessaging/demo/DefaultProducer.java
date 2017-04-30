
package io.openmessaging.demo;


import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageFactory;
import io.openmessaging.MessageHeader;
import io.openmessaging.Producer;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;

public class DefaultProducer implements Producer {
    private MessageFactory messageFactory = new DefaultMessageFactory();
    private MessageStore messageStore = MessageStore.getInstance();
    private KeyValue properties;
    private HashMap hashMap=new HashMap();
    public DefaultProducer(KeyValue properties) {

        this.properties = properties;
    }


    @Override public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {

        if(!hashMap.containsKey(topic)){
            File fileChild = new File(properties.getString("STORE_PATH") +"/"+MessageHeader.TOPIC+"/"+topic);
            try {
                fileChild.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        DefaultBytesMessage defaultBytesMessage= (DefaultBytesMessage) messageFactory.createBytesMessageToTopic(topic, body);
        String key= (String) properties.keySet().toArray()[0];

       defaultBytesMessage.putProperties(key,properties.getString(key));
        return defaultBytesMessage;

    }

    @Override public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {
        DefaultBytesMessage defaultBytesMessage=null;
        if(queue.substring(0,queue.indexOf("_")).equals("QUEUE")) {
            if (!hashMap.containsKey(queue)) {
                File fileChild = new File(properties.getString("STORE_PATH") + "/" + MessageHeader.QUEUE + "/" + queue);
                try {
                    fileChild.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
           defaultBytesMessage = (DefaultBytesMessage) messageFactory.createBytesMessageToQueue(queue, body);
        }else{
            if (!hashMap.containsKey(queue)) {
                File fileChild = new File(properties.getString("STORE_PATH") + "/" + MessageHeader.TOPIC + "/" + queue);
                try {
                    fileChild.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            defaultBytesMessage = (DefaultBytesMessage) messageFactory.createBytesMessageToTopic(queue, body);


        }
        String key= (String) properties.keySet().toArray()[0];

        defaultBytesMessage.putProperties(key,properties.getString(key));
        return defaultBytesMessage;
    }


    @Override public KeyValue properties() {
        return properties;
    }

    @Override public void send(Message message) {



        try {
            messageStore.putMessage(message,properties);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override public void send(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }



    @Override public void sendOneway(Message message) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void sendOneway(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

}