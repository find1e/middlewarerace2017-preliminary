
package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
public class MessageStore {

    private static final MessageStore INSTANCE = new MessageStore();
    private Map<String, ArrayList<Message>> messageBuckets = new HashMap<>();
    private Map<String, HashMap<String, Integer>> queueOffsets = new HashMap<>();
    private Lock lockFile=new ReentrantLock();
    private Map<String,InputStream> streamMap=new HashMap<>();

    public static MessageStore getInstance() {
        return INSTANCE;
    }

    public static void main(String[] args) throws FileNotFoundException, IOException{
        InputStream input=new FileInputStream("/storage/sdcard0/AppProjects/alinative/Queue/QUEUE1");
        byte[] b=new byte[input.available()];
        input.read(b);
        System.out.print(Arrays.toString(b));
    }

    public void putMessage(String bucket, Message message,KeyValue properties) throws IOException {
        String fileType=message.headers().containsKey(MessageHeader.TOPIC)?MessageHeader.TOPIC:MessageHeader.QUEUE;
        File file=new File(properties.getString("STORE_PATH")+"/"+fileType+"/"+bucket);
        FileOutputStream fileOutputStream = new FileOutputStream(file,true);

        FileChannel fileChannel = fileOutputStream.getChannel();


        DefaultBytesMessage defaultBytesMessage = (DefaultBytesMessage) message;

        ByteBuffer byteBuffer = ByteBuffer.allocate(defaultBytesMessage.getBody().length+1);

        byteBuffer.put(defaultBytesMessage.getBody());

        byteBuffer.put("#".getBytes());
        byteBuffer.flip();
        lockFile.lock();

        while(byteBuffer.hasRemaining()) {
            fileChannel.write(byteBuffer);
        }
        lockFile.unlock();
        fileChannel.close();
        fileOutputStream.close();
    }

    public synchronized Message pullMessage(String queue, String bucket,KeyValue KeyValue) {

        byte[] buff=new byte[256000];

 InputStream inputStream = null;
        //pull queue
        if (queue.equals(bucket))
        {
            String fileQueueLocal=KeyValue.getString("STORE_PATH") + "/" + MessageHeader.QUEUE + "/" + bucket;


            if(streamMap.containsKey(bucket)){

                inputStream=streamMap.get(bucket);
                if(inputStream==null){

                    return null;

                }
            }else{




                try
                {
                    inputStream=new FileInputStream(fileQueueLocal);
                }
                catch (FileNotFoundException e)
                {}

                streamMap.put(bucket,inputStream);
            }
            byte[] tempByte="#".getBytes();
            int temp=tempByte[0];
            int count=0;
            int b=0;
            try
            {
                b=inputStream.read();

                while (b != temp)
                {
                    buff[count++]=(byte) b;
                    b=inputStream.read();
                    if(b==-1){
                        System.out.println("@"+bucket+"over");
                        inputStream.close();
                        File file=new File(fileQueueLocal);
                        file.delete();

                        streamMap.put(bucket,null);

                        return null;

                    }
                }
            }catch(Exception e){}
            byte[] body=new byte[count];
            for(int indexNum=0;indexNum<count;indexNum++){
                body[indexNum]=buff[indexNum];
            }

            DefaultBytesMessage message=new DefaultBytesMessage(body);
            message.putHeaders(MessageHeader.QUEUE,queue);
            return message;
        }

        //pull topic
        String fileTopicLocal=KeyValue.getString("STORE_PATH") + "/" + MessageHeader.TOPIC + "/" + bucket;
        if(streamMap.containsKey(bucket)){

            inputStream=streamMap.get(bucket);
            if(inputStream==null){
                return null;

            }
        }else{




            try
            {
                inputStream=new FileInputStream(fileTopicLocal);
            }
            catch (FileNotFoundException e)
            {}

            streamMap.put(bucket,inputStream);
        }


        byte[] tempByte="#".getBytes();
        int temp=tempByte[0];


        int count=0;
        int b=0;
        try
        {
            b=inputStream.read();

            while (b != temp)
            {
                buff[count++]=(byte) b;
                b=inputStream.read();
                if(b==-1){
                    inputStream.close();
                    File file =new File(fileTopicLocal);
                    file.delete();
                    System.out.println("@"+bucket+"over");


                    streamMap.put(bucket,null);

                    return null;

                }
            }
        }catch(Exception e){}
        byte[] body=new byte[count];
        for(int indexNum=0;indexNum<count;indexNum++){
            body[indexNum]=buff[indexNum];
        }

        DefaultBytesMessage message=new DefaultBytesMessage(body);
        message.putHeaders(MessageHeader.TOPIC,bucket);


        return message;

    }
}




