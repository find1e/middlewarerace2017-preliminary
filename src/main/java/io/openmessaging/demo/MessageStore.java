package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class MessageStore {

    private static final MessageStore INSTANCE = new MessageStore();

    private Map<String,QueueProxy>  map = new HashMap();
    public static MessageStore getInstance() {
        return INSTANCE;
    }

    private Map<String, ArrayList<Message>> messageBuckets = new HashMap<>();

    private Map<String, HashMap<String, Integer>> queueOffsets = new HashMap<>();

    private ByteBuffer byteBuffer = ByteBuffer.allocate(SendConstants.buffSize);

    private AtomicInteger atomicIntegerFileName = new AtomicInteger(0);

    private AtomicBoolean atomicBooleanOverFlag = new AtomicBoolean(true);

    private HashMap<String,List<Integer>> threadIdMap =new HashMap();

    private AtomicInteger atomicIntegerThreadId = new AtomicInteger(1);

    private HashMap<Integer,Queue<DefaultBytesMessage>> queueMap = new HashMap(20);

    private AtomicBoolean flushFlag = new AtomicBoolean(true);

    private Semaphore semaphore = new Semaphore(20);


    private ReentrantLock reentrantLock = new ReentrantLock(true);




    public byte[][] serianized(DefaultBytesMessage message){


        String headerKey = message.headers().keySet().iterator().next();
        String headerValue = message.headers().getString(headerKey);
        Iterator iterator = message.properties().keySet().iterator();
        iterator.next();
        String propertiesKey = (String) iterator.next();
        String propertiesValue = message.properties().getString(propertiesKey);
        byte[] body = message.getBody();
        byte[] headerKeyByte = headerKey.getBytes();
        byte[] headerValueByte = headerValue.getBytes();
        byte[] propertiesKeyByte = propertiesKey.getBytes();
        byte[] propertiesValueByte = propertiesValue.getBytes();

        byte[][] messageByte = {headerKeyByte,headerValueByte,propertiesKeyByte,propertiesValueByte,body};
        return messageByte;
    }




    public void sendMessage(ByteBuffer byteBuffer,KeyValue properties){
        File file = new File(properties.getString("STORE_PATH")+"/"+atomicIntegerFileName.get());

        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        Path path = Paths.get(properties.getString("STORE_PATH")+"/"+atomicIntegerFileName.getAndAdd(1));
        AsynchronousFileChannel asynchronousFileChannel = null;
        try {
            asynchronousFileChannel = AsynchronousFileChannel.open(path, StandardOpenOption.WRITE);
        } catch (IOException e) {
            e.printStackTrace();
        }
        byteBuffer.flip();
        asynchronousFileChannel.write(byteBuffer, 0, asynchronousFileChannel, new CompletionHandler<Integer, AsynchronousFileChannel>() {
            @Override
            public void completed(Integer result, AsynchronousFileChannel attachment) {
                try {
                    attachment.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                semaphore.release();
            }

            @Override
            public void failed(Throwable exc, AsynchronousFileChannel attachment) {

                try {
                    attachment.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                semaphore.release();
            }
        });

    }


    public  void putMessage(DefaultBytesMessage message,KeyValue properties) {


        byte[][] messageByte = serianized(message);
        int length = 0;
        for (byte[] childByte : messageByte) {
            length += childByte.length;
            ++length;
        }

        reentrantLock.lock();
        if (length >= byteBuffer.remaining()) {
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            sendMessage(byteBuffer,properties);

            byteBuffer = ByteBuffer.allocate(SendConstants.buffSize);

        }
        for (byte[] childByte : messageByte) {
            byteBuffer.put(childByte);

            byteBuffer.put("$".getBytes()[0]);
        }
        reentrantLock.unlock();

    }

    public synchronized ByteBuffer deSerianied(KeyValue properties){
        File file = new File(properties.getString("STORE_PATH")+"/"+atomicIntegerFileName.getAndAdd(1));
        if (!file.exists()) {
            atomicBooleanOverFlag.compareAndSet(true,false);
            return null;

        }

        FileInputStream fileInputStream = null;
        FileChannel fileChannel = null;


        try {

           fileInputStream = new FileInputStream(file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();


        }

        try {
            fileChannel = fileInputStream.getChannel();
            byteBuffer.clear();
            fileChannel.read(byteBuffer);

        } catch (IOException e) {
            e.printStackTrace();

        }
        try {
            fileChannel.close();
            fileInputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


       /* Path path = Paths.get(properties.getString("STORE_PATH")+"/"+atomicIntegerFileName.getAndAdd(1));
        AsynchronousFileChannel asynchronousFileChannel = null;
        try {
            asynchronousFileChannel = AsynchronousFileChannel.open(path, StandardOpenOption.READ);
        } catch (IOException e) {
            e.printStackTrace();
        }

        Future future = asynchronousFileChannel.read(byteBuffer,0);

        while (!future.isDone());*/

        return byteBuffer;
    }
    public synchronized void insertMessage(ByteBuffer byteBuffer){
        byteBuffer.flip();
        byte[] buffBytes = byteBuffer.array();
        int cutCount = 1;
        int seek = 0;
        byte[] headerKeyByte = null;
        byte[] headerValueByte = null;
        byte[] propertiesKeyByte = null;
        byte[] propertiesValueByte = null;
        byte[] body = null;
        for (int indexNum = 0;indexNum < buffBytes.length;indexNum++) {

            if (buffBytes[indexNum] == SendConstants.cutFlag) {

                if (cutCount == 1) {
                    headerKeyByte = new byte[indexNum - seek];
                    for (int checkNum = 0;checkNum < headerKeyByte.length;checkNum++,seek++) {
                        headerKeyByte[checkNum] = buffBytes[seek];

                    }




                }
                if (cutCount == 2) {
                    headerValueByte = new byte[indexNum - seek];
                    for (int checkNum = 0;checkNum < headerValueByte.length;checkNum++,seek++) {
                        headerValueByte[checkNum] = buffBytes[seek];

                    }

                }
                if (cutCount == 3) {
                    propertiesKeyByte = new byte[indexNum - seek];
                    for (int checkNum = 0;checkNum < propertiesKeyByte.length;checkNum++,seek++) {
                        propertiesKeyByte[checkNum] = buffBytes[seek];

                    }

                }
                if (cutCount == 4) {
                    propertiesValueByte = new byte[indexNum - seek];
                    for (int checkNum = 0;checkNum < propertiesValueByte.length;checkNum++,seek++) {
                        propertiesValueByte[checkNum] = buffBytes[seek];

                    }

                }
                if (cutCount == 5) {
                    body = new byte[indexNum - seek];
                    for (int checkNum = 0;checkNum < body.length;checkNum++,seek++) {
                        body[checkNum] = buffBytes[seek];

                    }

                    String headerKey = new String(headerKeyByte);
                    String headerValue = new String(headerValueByte);
                    String propertiesKey = new String(propertiesKeyByte);
                    String propertiesValue = new String(propertiesValueByte);

                  //  System.out.println(headerKey+headerValue+propertiesKey+propertiesValue+new String(body));

                    DefaultBytesMessage defaultBytesMessage = new DefaultBytesMessage(body);
                    defaultBytesMessage.putHeaders(headerKey,headerValue);
                    defaultBytesMessage.putProperties(propertiesKey,propertiesValue);

                  /*  QueueProxy queueProxy = map.get(headerValue);
                    //TODO insert
                    if (queueProxy == null) {
                        queueProxy = new QueueProxy();
                        map.put(headerValue,queueProxy);

                    }
                    queueProxy.add(defaultBytesMessage);
*/
                /*  System.out.println("__________________________________________");
                  Iterator<Map.Entry<String, List<Integer>>> iterator = threadIdMap.entrySet().iterator();
                  while (iterator.hasNext()){
                      System.out.println(iterator.next().getKey());
                  }
                  System.out.println("--------------------------");
                  System.out.println("headerValue:"+headerValue);*/
                  List<Integer> list = threadIdMap.get(headerValue);
                   if (list == null) {
                      list = new ArrayList<Integer>();
                       threadIdMap.put(headerValue,list);

                   }
                    Queue queue = null;
                    for (int id : list) {

                        queue = queueMap.get(id);
                        queue.add(defaultBytesMessage);
                    }

                    cutCount = 0;
                }

                ++cutCount;
                ++seek;




            }


        }

    }
    public synchronized DefaultBytesMessage pullMessage(KeyValue properties,int threadId) {


        while (true) {
            Queue<DefaultBytesMessage> defaultBytesMessagesQueue = queueMap.get(threadId);
            DefaultBytesMessage defaultBytesMessage = defaultBytesMessagesQueue.poll();

            if (defaultBytesMessage == null && atomicBooleanOverFlag.get() == false) {
                return null;

            }
            if (defaultBytesMessage == null && atomicBooleanOverFlag.get() == true) {

                ByteBuffer byteBuffer = deSerianied(properties);
                if (byteBuffer == null) {


                    continue;
                }
                insertMessage(byteBuffer);

                continue;

            }

        /*    String headerKey = defaultBytesMessage.headers().keySet().iterator().next();
            String headerValue = defaultBytesMessage.headers().getString(headerKey);
            String propertiesKey = defaultBytesMessage.properties().keySet().iterator().next();
            String propertiesValue = defaultBytesMessage.properties().getString(propertiesKey);
            String body = new String(defaultBytesMessage.getBody());*/

            return defaultBytesMessage;
        }
    }

    public synchronized void attachInit(Collection<String> topics,String queue,KeyValue properties,int threadId){




            queueMap.put(threadId,new LinkedList<DefaultBytesMessage>());

        List<Integer> list = new ArrayList();
        list.add(threadId);
        threadIdMap.put(queue,list);

        for (String topic : topics) {
            List listTopic = threadIdMap.get(topic);
            if (listTopic == null) {
                listTopic = new ArrayList();
                listTopic.add(threadId);
                threadIdMap.put(topic,listTopic);

                continue;

            }
            listTopic.add(threadId);

        }
        /*
        QueueProxy queueProxyQueue = new QueueProxy();
        map.put(queue,queueProxyQueue);
  //      queueProxyQueue.attachQueue(queue);


        QueueProxy queueProxyTopic = null;
        for(String topic : topics) {
            queueProxyTopic = map.get(topic);

            if (queueProxyTopic == null) {
                queueProxyTopic = new QueueProxy();
                map.put(topic,queueProxyTopic);

            }
//            queueProxyTopic.attachTopics(topic);



*/
    }
   public void flush(KeyValue properties) {

        if (flushFlag.compareAndSet(true,false)) {
           // System.out.println("111");
            File file = new File(properties.getString("STORE_PATH") + "/" + atomicIntegerFileName.get());

            if (!file.exists()) {
                try {
                    file.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();

                }
            }
            if (byteBuffer.hasRemaining()) {
                Path path = Paths.get(properties.getString("STORE_PATH") + "/" + atomicIntegerFileName.getAndAdd(1));
                AsynchronousFileChannel asynchronousFileChannel = null;
                try {
                    asynchronousFileChannel = AsynchronousFileChannel.open(path, StandardOpenOption.WRITE);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                byteBuffer.flip();
                Future future = asynchronousFileChannel.write(byteBuffer, 0);

                while (!future.isDone()) ;
                try {
                    asynchronousFileChannel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }

            atomicIntegerFileName.set(0);
            System.out.println("wwwwwwwwwwww");
        }
           }


    public AtomicInteger getAtomicIntegerThreadId() {
        return atomicIntegerThreadId;
    }

    public void setAtomicIntegerThreadId(AtomicInteger atomicIntegerThreadId) {
        this.atomicIntegerThreadId = atomicIntegerThreadId;
    }

    public HashMap getQueueMap() {
        return queueMap;
    }

    public void setQueueMap(HashMap queueMap) {
        this.queueMap = queueMap;
    }
}
