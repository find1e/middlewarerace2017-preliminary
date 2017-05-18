
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

            File file = new File(properties.getString("STORE_PATH") + "/" + "Queue");

            try {
                file.createNewFile();
                for(int checkNum=0;checkNum<20;checkNum++){
                    File bucketFile=new File(file.getAbsolutePath()+"/"+"QUEUE_"+checkNum);
                    bucketFile.createNewFile();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }


        File file2 = new File(properties.getString("STORE_PATH") + "/" + "Topic");

        try {
            file2.createNewFile();
            for(int checkNum=0;checkNum<90;checkNum++){
                File bucketFile=new File(file2.getAbsolutePath()+"/"+"TOPIC_"+checkNum);
                bucketFile.createNewFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }



        if(messageStore.getSendMap()==null){

            messageStore.setSendMap(new HashMap(120));
            File file3=new File(properties.getString("STORE_PATH"));

            File[] files=file3.listFiles();
            for(File f:files){
                String[] fileNames=f.list();
                for(String fileName:fileNames){
                    FileChannelProxy fileChannelProxy=new FileChannelProxy();
                    FileOutputStream fileOutputStream=null;
                    try {
                        fileOutputStream=new FileOutputStream(f.getAbsolutePath()+"/"+fileName);
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    }
                    FileChannel fileChannel=fileOutputStream.getChannel();
                    fileChannelProxy.setFileChannel(fileChannel);
                    fileChannelProxy.setFileOutputStream(fileOutputStream);

                    HashMap sendMap=messageStore.getSendMap();
                    sendMap.put(fileName,fileChannelProxy);

                }

            }

        }

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