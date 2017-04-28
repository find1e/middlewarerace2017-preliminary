package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import io.openmessaging.StreamCallBack;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.Channel;
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



public class MessageStore  {

    private static final MessageStore INSTANCE = new MessageStore();

    public static MessageStore getInstance() {
        return INSTANCE;
    }

    private Map<String, ArrayList<Message>> messageBuckets = new HashMap<>();

    private Map<String, HashMap<String, Integer>> queueOffsets = new HashMap<>();

    private Lock lockFile = new ReentrantLock();

    private Lock lockGet=new ReentrantLock();

    private Lock lockRead=new ReentrantLock();

    private HashMap<String,FileChannelProxy> queueMap = new HashMap(20);

    private DistributeLock distributeLock=new DistributeLock(100);

    private DistributeLock sendLock=new DistributeLock(120);

    final ThreadLocal threadLocal=new ThreadLocal();


    public  void putMessage(String bucket, Message message, KeyValue properties) throws IOException {

        String fileType = message.headers().containsKey(MessageHeader.TOPIC) ? MessageHeader.TOPIC : MessageHeader.QUEUE;
        String fileLocal=properties.getString("STORE_PATH") + "/" + fileType + "/" + bucket;

        threadLocal.set(fileLocal);
        FileChannelProxy fileChannelProxy=null;
        StreamCallBack callBack=new StreamCallBack() {
            @Override
            public FileChannelProxy callBack() {
                FileOutputStream fileOutputStream= null;
                try {
                    fileOutputStream = new FileOutputStream((String) threadLocal.get(),true);
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
                FileChannel fileChannel=fileOutputStream.getChannel();
                FileChannelProxy fileChannelProxy=new FileChannelProxy();
                fileChannelProxy.setFileChannel(fileChannel);
                fileChannelProxy.setFileOutputStream(fileOutputStream);
                return fileChannelProxy;
            }
        };
        while((fileChannelProxy=sendLock.lock(bucket,callBack))==null);

        FileChannel fileChannel=fileChannelProxy.getFileChannel();

        DefaultBytesMessage defaultBytesMessage = (DefaultBytesMessage) message;
        byte[] body=defaultBytesMessage.getBody();
        int length=body.length;
        ByteBuffer byteBuffer = ByteBuffer.allocate(length + 2);

        int i=0;//i��ʾ1�ֽڿ��Ա�ʾ�����ִ�С��
        int j=0;//j��ʾ�������ٸ��ֽ�
        byte[] lenFlag=new byte[2];
        if(length>255){
        j=length/255;
        }
        i=length;
        lenFlag[0]= (byte) j;
        lenFlag[1]= (byte) i;
        byteBuffer.put(lenFlag);
        byteBuffer.put(defaultBytesMessage.getBody());

        byteBuffer.flip();

        while (byteBuffer.hasRemaining()) {
            fileChannel.write(byteBuffer);
        }
        sendLock.unLock(fileChannelProxy);
    }

    public static void main(String[] args) throws FileNotFoundException, IOException {


    }
    public  MessageProxy pullMessage(String queue, String bucket, KeyValue keyValue) {


        String queueLocal = keyValue.getString("STORE_PATH") + "/" + MessageHeader.QUEUE + "/" + bucket;
        String topicLocal = keyValue.getString("STORE_PATH") + "/" + MessageHeader.TOPIC + "/" + bucket;

        if(queue.equals(bucket)) {
            return readQueue(queueLocal, queue,keyValue);
        }else {

            return readTopic(topicLocal,bucket,keyValue);

        }
    }

    public  MessageProxy readQueue(String fileLocal,String bucket,KeyValue keyValue) {


        FileInputStream fileInputStream = null;
        FileChannelProxy inputStream = null;
        FileChannel fileChannel = null;
        if (queueMap.containsKey(bucket)) {
            inputStream = queueMap.get(bucket);

        } else {
            try {
                fileInputStream = new FileInputStream(fileLocal);
                inputStream = new FileChannelProxy();
                inputStream.setFileChannel(fileInputStream.getChannel());
                inputStream.setFileInputStream(fileInputStream);
            } catch (FileNotFoundException e) {

                e.printStackTrace();
            }
            queueMap.put(bucket, inputStream);
        }

        ByteBuffer preBuff = ByteBuffer.allocate(2);
        try {
            fileChannel = inputStream.getFileChannel();

            if (fileChannel.read(preBuff) == -1) {
                System.out.println("@" + bucket + "over");


                fileChannel.close();

                inputStream.getFileInputStream().close();
                File file = new File(fileLocal);



                return new MessageProxy(true);

            }


            //  System.out.println(Arrays.toString(preBuff.array()));
            preBuff.flip();
            byte[] lenFlag = preBuff.array();
            int len = 0;
            if (lenFlag[0] != 0) {
                int temp = lenFlag[0] * 255;
                len += temp;
            }
            len += lenFlag[1];

            ByteBuffer buff = ByteBuffer.allocate(len);
            //System.out.println("����"+len);

            fileChannel.read(buff);

            DefaultBytesMessage message = new DefaultBytesMessage(buff.array());
            message.putHeaders(MessageHeader.QUEUE, bucket);
            message.putProperties(keyValue);


            return new MessageProxy(message);

        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }







    public  MessageProxy readTopic(String fileLocal,String bucket,KeyValue keyValue){


        threadLocal.set(fileLocal);
        StreamCallBack streamCallBack=new StreamCallBack() {
            @Override
            public FileChannelProxy callBack(){
                FileInputStream fileInputStream=null;
                FileChannel fileChannel=null;
                try {
                   fileInputStream=new FileInputStream((String) threadLocal.get());
                    fileChannel=fileInputStream.getChannel();
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
              FileChannelProxy fileChannelProxy=new FileChannelProxy();
                fileChannelProxy.setFileInputStream(fileInputStream);
                fileChannelProxy.setFileChannel(fileChannel);
                return fileChannelProxy;
            }


        };
        FileChannelProxy fileChannelProxy=distributeLock.lock(bucket,streamCallBack);

        if(fileChannelProxy==null){

            return null;

        }

        FileChannel fileChannel = null;




      /*  if (topicMap.containsKey(bucket)) {//judge and set
            inputStream = topicMap.get(bucket);

        } else {
            try {
                fileInputStream = new FileInputStream(fileLocal);
                inputStream = new FileChannelProxy();
                inputStream.setFileChannel(fileInputStream.getChannel());
                inputStream.setFileInputStream(fileInputStream);
            } catch (FileNotFoundException e) {

                e.printStackTrace();
            }
            topicMap.put(bucket, inputStream);
        }*/

         if(fileChannelProxy.isEnd()){
             MessageProxy messageProxy=new MessageProxy(true);
             distributeLock.unLock(fileChannelProxy);
             return messageProxy;
         }
        ByteBuffer preBuff = ByteBuffer.allocate(2);
        try {
            fileChannel = fileChannelProxy.getFileChannel();

            if (fileChannel.read(preBuff) == -1) {

                System.out.println("@" + bucket + "over");


                fileChannel.close();

                fileChannelProxy.getFileInputStream().close();
                fileChannelProxy.setEnd(true);
                File file = new File(fileLocal);

               //todo remove filechannelproxy on lockmap

                MessageProxy messageProxy=new MessageProxy(true);
                distributeLock.unLock(fileChannelProxy);
                return messageProxy;

            }


            //  System.out.println(Arrays.toString(preBuff.array()));
            preBuff.flip();
            byte[] lenFlag = preBuff.array();
            int len = 0;
            if (lenFlag[0] != 0) {
                int temp = lenFlag[0] * 255;
                len += temp;
            }
            len += lenFlag[1];

            ByteBuffer buff = ByteBuffer.allocate(len);
            //System.out.println("����"+len);

            fileChannel.read(buff);

            DefaultBytesMessage message = new DefaultBytesMessage(buff.array());
            message.putHeaders(MessageHeader.TOPIC, bucket);
            message.putProperties(keyValue);


            distributeLock.unLock(fileChannelProxy);
            return new MessageProxy(message);

        } catch (IOException e) {
            e.printStackTrace();
        }
        distributeLock.unLock(fileChannelProxy);

        return null;
    }


 /*   public static void main(String[] args) throws FileNotFoundException, IOException {
      *//*  int m=7;
        int n=90;
         m=m<<1+8;
        System.out.println(m>>2);
        BitSet bitSet=new BitSet();*//*

      RandomAccessFile random=new RandomAccessFile("D:\\race\\Queue\\test.txt","r");
        Mytask task=new Mytask();
        Mytask task2=new Mytask();
        ReentrantLock reen=new ReentrantLock();
        task2.random=random;
        task2.lock=reen;
        task2.i=0;
        task.random=random;
        task.lock=reen;
        task.i=5;
        Thread thread=new Thread(task);
        Thread thread2=new Thread(task2);
        thread.start();
        thread2.start();

      *//*

        FileInputStream input=new FileInputStream("D:\\race\\Queue\\test.txt");

        FileChannel fileChannel=input.getChannel();
        System.out.println(fileChannel.position());
        FileChannel file=fileChannel.position(5);
        ByteBuffer byteBuffer=ByteBuffer.allocate(20);
        file.read(byteBuffer);

        System.out.println(Arrays.toString(byteBuffer.array()));

        FileChannel channel=input.getChannel();
        ByteBuffer b=ByteBuffer.allocate(20);
        channel.position(6);
        channel.read(b);
        System.out.println(Arrays.toString(b.array()));
*//*


        *//*ReentrantLock lock=new ReentrantLock();
        Mytask myTask=new Mytask();
        myTask.input=input;
        myTask.lock=lock;
        Thread thread1=new Thread(myTask);
        Thread thread2=new Thread(myTask);
        thread1.start();
        thread2.start();
*//*

    }


}


class Mytask implements  Runnable{
    FileInputStream input;
    ReentrantLock lock;
    RandomAccessFile random;
    int i;
    public void read() {
        lock.lock();
        try {
            random.seek(i);

            for(int indexNum=0;indexNum<10;indexNum++) {



            }
            random.seek(0);

            } catch (IOException e) {
            e.printStackTrace();
        }
        lock.unlock();
    }
    @Override
    public void run() {
        read();
    }*/
}


