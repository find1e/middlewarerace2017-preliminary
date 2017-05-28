package io.openmessaging.demo;


import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;


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

    private AtomicInteger atomicIntegerFileName = new AtomicInteger(0);

    private AtomicBoolean atomicBooleanOverFlag = new AtomicBoolean(true);

    private HashMap<String,List<Integer>> threadIdMap =new HashMap();

    private AtomicInteger atomicIntegerThreadId = new AtomicInteger(1);

    private HashMap<Integer,Queue<DefaultBytesMessage>> queueMap = new HashMap(20);

    private AtomicBoolean flushFlag = new AtomicBoolean(true);

    private Semaphore semaphore = new Semaphore(20,true);

    private ReentrantLock reentrantLock = new ReentrantLock(true);

    private ByteBuffer byteBuffer = ByteBuffer.allocate(SendConstants.buffSize);


    public  byte[] serianized(DefaultBytesMessage message){

        StringBuilder stringBuilder = new StringBuilder();
        for (String key : message.headers().keySet()){
            stringBuilder.append(key);
            stringBuilder.append("%");
            stringBuilder.append(message.headers().getString(key));
            stringBuilder.append("#");

        }
        stringBuilder.append(SendConstants.cutChar);
        for (String propertiesKey : message.properties().keySet()) {
            stringBuilder.append(propertiesKey);
            stringBuilder.append("%");
            stringBuilder.append(message.properties().getString(propertiesKey));
            stringBuilder.append("#");

        }
        stringBuilder.append(SendConstants.cutChar);


        byte[] headerPropertiesByte = stringBuilder.toString().getBytes();
        byte[] body = message.getBody();
        /*String bodyString = new String (body);
        if ("PRODUCER_4_27".equals(bodyString)) {
            System.out.println("+1");

        }*/
        byte[] messageByte = new byte[headerPropertiesByte.length+body.length+1];
        for (int index = 0;index < headerPropertiesByte.length;index++) {
            messageByte[index] = headerPropertiesByte[index];
        }
        for (int index = 0,indexNum = headerPropertiesByte.length;index < body.length;index++,indexNum++) {
            messageByte[indexNum] = body[index];

        }
        messageByte[messageByte.length - 1] = SendConstants.cutFlag;
        String s = new String(messageByte);

        return messageByte;
    }




    public  void sendMessage(ByteBuffer byteBuffer,KeyValue properties){



        byteBuffer.flip();
        File file = new File(properties.getString("STORE_PATH") + "/" + atomicIntegerFileName.get());

        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();

            }
        }

            Path path = Paths.get(properties.getString("STORE_PATH") + "/" + atomicIntegerFileName.getAndAdd(1));
            AsynchronousFileChannel asynchronousFileChannel = null;
            try {
                asynchronousFileChannel = AsynchronousFileChannel.open(path, StandardOpenOption.WRITE);
            } catch (IOException e) {
                e.printStackTrace();
            }

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


    public synchronized void putMessage(DefaultBytesMessage message,KeyValue properties) {







        byte[] messageByte = serianized(message);



        if (messageByte.length >= byteBuffer.remaining()) {

            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            sendMessage(byteBuffer,properties);

            byteBuffer = ByteBuffer.allocate(SendConstants.buffSize);



        }

        byteBuffer.put(messageByte);



    }


        /*public boolean putAndAllocate(boolean remainFlag,boolean allocateFlag,boolean putFlag,byte[] messageByte){

        reentrantLock.lock();
        if (remainFlag == true) {
           boolean result = messageByte.length > byteBuffer.remaining();
           reentrantLock.unlock();
            return result;

        }

        if (allocateFlag == true) {
            byteBuffer = ByteBuffer.allocate(SendConstants.buffSize);
            reentrantLock.unlock();
            return true;
        }

        if (putFlag == true) {
            byteBuffer.put(messageByte);
            reentrantLock.unlock();
            return true;
        }



        return true;

        }
*/
    public  ByteBuffer deSerianied(KeyValue properties){
        File file = new File(properties.getString("STORE_PATH") +"/"+atomicIntegerFileName.getAndAdd(1));
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
        ByteBuffer byteBuffer = null;
        try {
            fileChannel = fileInputStream.getChannel();
          byteBuffer = ByteBuffer.allocate(SendConstants.buffSize);
            fileChannel.read(byteBuffer);

            byteBuffer.flip();
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


/*
    public static void main (String[] args) {
        MessageStore main = new MessageStore();
        DefaultBytesMessage defaultBytesMessage = new DefaultBytesMessage("hello".getBytes());
        defaultBytesMessage.putHeaders("topic","TOPIC_1");
        defaultBytesMessage.putProperties("ppp","kkk");
        defaultBytesMessage.putProperties("STORE_PATH","/home/fbhw/race");

        DefaultBytesMessage defaultBytesMessage1 = new DefaultBytesMessage(null);
        byte[] buffBytes = main.serianized(defaultBytesMessage);
        int cutCount = 1;
        int seek = 0;
        byte[] headerByte = null;
        byte[] propertiesByte = null;
        byte[] body = null;
        String headerString = null;
        String propertiesString = null;
        for (int indexNum = 0;indexNum < buffBytes.length;indexNum++) {

            if (buffBytes[indexNum] == SendConstants.cutFlag) {

                if (cutCount == 1) {

                    headerByte = new byte[indexNum - seek];

                    for (int checkNum = 0;checkNum < headerByte.length;checkNum++,seek++) {
                        headerByte[checkNum] = buffBytes[seek];

                    }
                  //  System.out.println(Arrays.toString(headerByte));
                    int seekChild = 0;
                    for (int index = 0;index < headerByte.length;index++) {
                        if (headerByte[index] == SendConstants.cutChild) {

                            byte[] header = new byte[index - seekChild];
                            index++;

                            for (int i = 0;i < header.length;i++,seekChild++) {
                                header[i] = headerByte[seekChild];


                            }
                            headerString = new String(header);
                            System.out.println(headerString);
                            String[] headers = headerString.split(":");
                            String headerKey = headers[0];

                            String headerValue = headers[1];
                           // System.out.println(headerKey+headerValue);
                            defaultBytesMessage1.putHeaders(headerKey,headerValue);


                            seekChild++;
                        }


                    }




                }

                if (cutCount == 2) {

                    propertiesByte = new byte[indexNum - seek];

                    for (int checkNum = 0; checkNum <propertiesByte.length; checkNum++, seek++) {
                        propertiesByte[checkNum] = buffBytes[seek];

                    }
                   // System.out.println(Arrays.toString(propertiesByte));
                    int seekChild = 0;
                    for (int index = 0; index < propertiesByte.length; index++) {
                        if (propertiesByte[index] == SendConstants.cutChild) {


                            byte[] properties = new byte[index - seekChild];
                            index++;
                            for (int i = 0; i < properties.length; i++, seekChild++) {
                                properties[i] = propertiesByte[seekChild];


                            }
                            propertiesString = new String(properties);

                            System.out.println(properties);
                            String[] propertie = propertiesString.split(":");
                            String propertiesKey = propertie[0];
                            String propertiesValue = propertie[1];
                          */
/*  System.out.println(propertiesKey);
                            System.out.println(propertiesValue);*//*

                            defaultBytesMessage1.putProperties(propertiesKey,propertiesValue);

                            seekChild++;
                        }


                    }


                }

                    if (cutCount == 3) {
                    body = new byte[indexNum - seek];
                    for (int checkNum = 0; checkNum < body.length; checkNum++, seek++) {
                        body[checkNum] = buffBytes[seek];

                    }


                    defaultBytesMessage1.setBody(body);



                    cutCount = 0;
                }

                ++cutCount;
                ++seek;
                }



        }
System.out.println("======");
System.out.println(defaultBytesMessage1.headers().getString("topic"));
       System.out.println(defaultBytesMessage1.properties().getString("STORE_PATH"));
        System.out.println(defaultBytesMessage1.properties().getString("ppp"));
        System.out.println(new String(defaultBytesMessage1.getBody()));


    }
*/
    public synchronized void insertMessage(ByteBuffer byteBuffer){
        //byteBuffer.flip();
        byte[] buffBytes = byteBuffer.array();


        //System.out.println(buffBytes[99996]);
       /* byte[] test = new byte[112];
        for (int j = 0,i = 99885;i < 99996;i++,j++) {

            test[j] = buffBytes[i];


        }
        System.out.println(new String(test));
       */ int cutCount = 1;
        int seek = 0;
        byte[] headerByte = null;
        byte[] propertiesByte = null;
        byte[] body = null;

        String[] headers = null;
        DefaultBytesMessage defaultBytesMessage1 = new DefaultBytesMessage(null);
        for (int indexNum = 0;buffBytes[indexNum] != 0;indexNum++) {

            if (buffBytes[indexNum] == SendConstants.cutFlag) {

                if (cutCount == 1) {

                    headerByte = new byte[indexNum - seek];


                    for (int checkNum = 0;checkNum < headerByte.length;checkNum++,seek++) {
                        headerByte[checkNum] = buffBytes[seek];

                    }
                    //  System.out.println(Arrays.toString(headerByte));
                    int seekChild = 0;
                    for (int index = 0;index < headerByte.length;index++) {
                        if (headerByte[index] == SendConstants.cutChild) {

                            byte[] header = new byte[index - seekChild];
                            index++;

                            for (int i = 0;i < header.length;i++,seekChild++) {
                                header[i] = headerByte[seekChild];


                            }
                            byte[] headerKeyByte = null;
                            byte[] headerValueByte = null;
                            int flag = 0;
                            for (int j = 0; j < header.length;j++) {
                                if (header[j] == "%".getBytes()[0]) {
                                    flag = j;
                                    headerKeyByte = new byte[j];
                                    headerValueByte = new byte[header.length - j - 1];


                                    if (headerValueByte == null) {
                                        System.out.println("headerValueByte" + headerValueByte);
                                        System.out.println();
                                    }
                                    if (headerKeyByte == null) {
                                        System.out.println("headerKeyByte" + headerKeyByte);

                                    }


                                    for (int q = 0; q < flag; q++) {

                                        headerKeyByte[q] = header[q];

                                    }
                                    for (int w = 0, e = flag + 1; e < header.length; e++, w++) {


                                        headerValueByte[w] = header[e];


                                    }

                                    String s = new String (new String(headerKeyByte));
                                    /*if (s.length() <2) {

                                        System.out.print(new String(headerByte));
                                        System.out.print("shangcibody:"+new String(body));
                                        System.out.println("and"+buffBytes.length);

                                    }
*/
//                                    System.out.print(new String(headerKeyByte));
//                                    System.out.print("-");
//                                    System.out.print(new String(headerValueByte));
//                                    System.out.print("-");
//                                    //   System.out.println(new String(headerKeyByte)+new String(headerValueByte));
                                    defaultBytesMessage1.putHeaders(new String(headerKeyByte), new String(headerValueByte));

                                }

                            }
                            seekChild++;
                        }


                    }




                }

                if (cutCount == 2) {

                    propertiesByte = new byte[indexNum - seek];

                    for (int checkNum = 0; checkNum <propertiesByte.length; checkNum++, seek++) {
                        propertiesByte[checkNum] = buffBytes[seek];

                    }
                    // System.out.println(Arrays.toString(propertiesByte));
                    int seekChild = 0;
                    for (int index = 0; index < propertiesByte.length; index++) {
                        if (propertiesByte[index] == SendConstants.cutChild) {


                            byte[] properties = new byte[index - seekChild];
                            index++;
                            for (int i = 0; i < properties.length; i++, seekChild++) {
                                properties[i] = propertiesByte[seekChild];


                            }
                            byte[] propertiesKeyByte = null;
                            byte[] propertiesValueByte = null;
                            int flag = 0;
                            for (int j = 0; j < properties.length;j++) {
                                if (properties[j] == "%".getBytes()[0]) {
                                    flag = j;
                                    propertiesKeyByte = new byte[j];
                                     propertiesValueByte = new byte[properties.length - j - 1];


                            for (int q = 0; q < flag;q++) {

                                    propertiesKeyByte[q] = properties[q];

                                }
                                for (int p = 0,q = flag + 1;q<properties.length;q++,p++) {

                                    propertiesValueByte[p] = properties[q];


                            }




                          //  System.out.print(defaultBytesMessage1);
                          //  System.out.println(new String(propertiesKeyByte)+ new String(propertiesValueByte));

                               /*     System.out.print(new String(propertiesKeyByte));

                            System.out.print("-");
                                    System.out.print(new String(propertiesValueByte));
                                    System.out.print("-");
                           */ defaultBytesMessage1.putProperties(new String(propertiesKeyByte), new String(propertiesValueByte));


                        }

                            }
                            seekChild++;
                        }


                    }


                }

                if (cutCount == 3) {
                    body = new byte[indexNum - seek];

                    for (int checkNum = 0; checkNum < body.length; checkNum++, seek++) {
                        body[checkNum] = buffBytes[seek];

                    }

                  //  System.out.println(new String(body));

                   // System.out.println(new String(body));
                    defaultBytesMessage1.setBody(body);
                  /*  String headerKey = defaultBytesMessage1.headers().keySet().iterator().next();
                    String headerValue = defaultBytesMessage1.headers().getString(headerKey);
                    String propert = defaultBytesMessage1.properties().keySet().iterator().next();
                    String properti = defaultBytesMessage1.properties().getString(propert);
                    String bod = new String(defaultBytesMessage1.getBody());

                    System.out.println(headerKey+"-"+headerValue+"-"+propert+"-"+properti+"-"+new String(bod));
*/


                        String bucket = defaultBytesMessage1.headers().keySet().contains(MessageHeader.TOPIC)?defaultBytesMessage1.headers().getString(MessageHeader.TOPIC):defaultBytesMessage1.headers().getString(MessageHeader.QUEUE);

                    List<Integer> list = threadIdMap.get(bucket);
                    if (list == null) {
                        list = new ArrayList<Integer>();
                        threadIdMap.put(bucket,list);

                    }
                    Queue queue = null;
                    for (int id : list) {

                        queue = queueMap.get(id);
                        queue.add(defaultBytesMessage1);
                    }


                    if ((indexNum + 1) < buffBytes.length){
                        defaultBytesMessage1 = new DefaultBytesMessage(null);
                    }
                    cutCount = 0;
                }

                ++cutCount;
                ++seek;
            }









        }

    }

    public  void slipString (DefaultBytesMessage defaultBytesMessage1,byte[] header) {
        String  headerStirng = new String(header);


        int ind = headerStirng.indexOf("%");
        //System.out.println(headerStirng);
        String headerKey = headerStirng.substring(0,ind);
        String  headerValue = headerStirng.substring(ind + 1,headerStirng.length() - ind - 1);

        //  System.out.println(headerKey+headerValue);
        defaultBytesMessage1.putHeaders(headerKey, headerValue);

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

            /*String headerKey = defaultBytesMessage.headers().keySet().iterator().next();
            String headerValue = defaultBytesMessage.headers().getString(headerKey);
            String propertiesKey = defaultBytesMessage.properties().keySet().iterator().next();
            String propertiesValue = defaultBytesMessage.properties().getString(propertiesKey);
            String body = new String(defaultBytesMessage.getBody());*/
            //System.out.println(defaultBytesMessage);

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
