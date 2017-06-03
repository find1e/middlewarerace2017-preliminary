package io.openmessaging.demo;

import io.openmessaging.KeyValue;
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
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class MessageStore {

    private static final MessageStore INSTANCE = new MessageStore();


    public static MessageStore getInstance() {
        return INSTANCE;
    }

    private AtomicInteger atomicIntegerFileName = new AtomicInteger(0);

    private AtomicBoolean atomicBooleanOverFlag = new AtomicBoolean(true);

    private final HashMap<String,List<Integer>> threadIdMap =new HashMap(110);

    private AtomicInteger atomicIntegerThreadId = new AtomicInteger(1);

    private final HashMap<Integer,Queue<DefaultBytesMessage>> queueMap = new HashMap(20);

    private AtomicBoolean flushFlag = new AtomicBoolean(true);

    private Semaphore semaphore = new Semaphore(1);

    private ByteBuffer byteBuffer = ByteBuffer.allocateDirect(SendConstants.buffSize);

    private ByteBuffer byteBuffer2 = ByteBuffer.allocateDirect(SendConstants.buffSize);

    private ByteBuffer resultBuffer = null;

    private String[] headerStrings = null;

    private String[] propertiesStrings = null;

    // private ReentrantLock reentrantLock = new ReentrantLock();


   // private AtomicBoolean insertFlag = new AtomicBoolean(true);




    public  byte[] serianized(DefaultBytesMessage message,KeyValue properties){

        if (atomicIntegerFileName.get() == 0) {
            Set headerKeySet = message.headers().keySet();

            int headNum = headerKeySet.size();


            byte[][] headerKeyByte = new byte[headNum][];

            Iterator<String> iterator = headerKeySet.iterator();
            int indexNum = 0;
            while (iterator.hasNext()) {

                String headerKey = iterator.next();
                headerKeyByte[indexNum++] = headerKey.getBytes();
            }
            Set propertiesKeySet = message.properties().keySet();
            int propertiesNum = propertiesKeySet.size();

            byte[][] propertiesKeyByte = new byte[propertiesNum][];

            Iterator<String> i = propertiesKeySet.iterator();
            int index = 0;
            while (i.hasNext()){
                String propertiesKey = i.next();
                 propertiesKeyByte[index++] = propertiesKey.getBytes();



            }
            int  length = 0;
            for (byte[] b : headerKeyByte) {
                length += b.length;

                ++length;
            }
            for (byte[] b : propertiesKeyByte) {
                length += b.length;
                ++length;
            }
            byte[] messageByte = new byte[length + 2];
            int num = 0;
            messageByte[num++] = (byte)headNum;
            messageByte[num++] = (byte)propertiesNum;



            for (int ind = 0;ind < headerKeyByte.length;ind++) {
                byte len = (byte) headerKeyByte[ind].length;
                messageByte[num++] = len;
                for (int check = 0;check < headerKeyByte[ind].length;check++) {
                    messageByte[num++] = headerKeyByte[ind][check];

                }





            }

            for (int ind = 0;ind < propertiesKeyByte.length;ind++) {
                byte len = (byte) propertiesKeyByte[ind].length;
                messageByte[num++] = len;
                for (int check = 0;check < propertiesKeyByte[ind].length;check++) {
                    messageByte[num++] = propertiesKeyByte[ind][check];

                }


            }
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(messageByte.length);
            byteBuffer.put(messageByte);


            File file = new File(properties.getString("STORE_PATH") + "/" + "keys");


            if (!file.exists()) {
                try {
                    file.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();

                }
            }

            Path path = Paths.get(file.getAbsolutePath());

            AsynchronousFileChannel asynchronousFileChannel = null;

            try {
                asynchronousFileChannel = AsynchronousFileChannel.open(path, StandardOpenOption.WRITE);
            } catch (IOException e) {
                e.printStackTrace();
            }





            byteBuffer.flip();


            asynchronousFileChannel.write(byteBuffer, 0,asynchronousFileChannel, new CompletionHandler<Integer,AsynchronousFileChannel>() {
                @Override
                public void completed(Integer result, AsynchronousFileChannel attachment) {
                    try {
                        attachment.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }



                }

                @Override
                public void failed(Throwable exc, AsynchronousFileChannel attachment) {

                    try {
                        attachment.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
            });


        }


        Set headerKeySet = message.headers().keySet();

        int headNum = headerKeySet.size();



        byte[][] headerValueByte = new byte[headNum][];
        Iterator<String> iterator = headerKeySet.iterator();
        int indexNum = 0;
        while (iterator.hasNext()){

            String headerKey = iterator.next();
            String headerValue = message.headers().getString(headerKey);

            headerValueByte[indexNum++] = headerValue.getBytes();

        }
/*FileChannel fileChannel;
        MappedByteBuffer map = fileChannel.map(new FileChannel.MapMode(), 0, 0);
        map.array();*/
        Set propertiesKeySet = message.properties().keySet();
        int propertiesNum = propertiesKeySet.size();


        byte[][] propertiesValueByte = new byte[propertiesNum][];
        Iterator<String> i = propertiesKeySet.iterator();
        int index = 0;
        while (i.hasNext()){
            String propertiesKey = i.next();
            String propertiesValue = message.properties().getString(propertiesKey);

            propertiesValueByte[index++] = propertiesValue.getBytes();



        }



        byte[] body = message.getBody();

       int  length = body.length;

        for (byte[] b : headerValueByte) {
            length += b.length;

            length = length + 3;
        }

        for (byte[] b : propertiesValueByte) {
            length += b.length;

            length = length + 3;
        }



        byte[] messageByte = new byte[length + 5];
        int num = 0;
        messageByte[num++] = (byte)headNum;
        messageByte[num++] = (byte)propertiesNum;



        for (int ind = 0;ind < headerValueByte.length;ind++) {





            int len2 =  headerValueByte[ind].length;
            int j=0;//j��ʾ�������ٸ��ֽ�

            int h=0;

            if(len2>16129){
                h = len2/16129;
                len2 = len2%16129;
            }
            if(len2>127){
                j = len2/127;
                len2 = len2%127;
            }

            messageByte[num++] = (byte) h;
            messageByte[num++] = (byte) j;
            messageByte[num++] = (byte) len2;




            for (int check2 = 0;check2 < headerValueByte[ind].length;check2++) {
                messageByte[num++] = headerValueByte[ind][check2];

            }

        }


        for (int ind = 0;ind < propertiesValueByte.length;ind++) {


            int len2 =  propertiesValueByte[ind].length;
            int j=0;//j��ʾ�������ٸ��ֽ�
            int h=0;

            if(len2>16129){
                h = len2/16129;
                len2 = len2%16129;
            }
            if(len2>127){
                j = len2/127;
                len2 = len2%127;
            }
            messageByte[num++] = (byte) h;
            messageByte[num++] = (byte) j;
            messageByte[num++] = (byte) len2;
            for (int check2 = 0;check2 < propertiesValueByte[ind].length;check2++) {
                messageByte[num++] = propertiesValueByte[ind][check2];

            }

        }


        int len =  body.length;
        int j=0;//j��ʾ�������ٸ��ֽ�
        int h=0;

        if(len>16129){
            h = len/16129;
            len = len%16129;
        }
        if(len>127){
            j = len/127;
            len = len%127;
        }

        messageByte[num++] = (byte) h;
        messageByte[num++] = (byte) j;
        messageByte[num++] = (byte) len;

        for (int bodyIndex = 0; bodyIndex < body.length;bodyIndex++) {
            messageByte[num++] = body[bodyIndex];
        }

                return messageByte;
    }




    public void sendMessage(ByteBuffer byteBuffer,KeyValue properties,Semaphore reentrantLock){

        File file = new File(properties.getString("STORE_PATH") + "/" + atomicIntegerFileName.getAndAdd(1));


        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();

            }
        }

            Path path = Paths.get(file.getAbsolutePath());

            AsynchronousFileChannel asynchronousFileChannel = null;

            try {
                asynchronousFileChannel = AsynchronousFileChannel.open(path, StandardOpenOption.WRITE);
            } catch (IOException e) {
                e.printStackTrace();
            }



            byte[] putByte = new byte[byteBuffer.capacity() - byteBuffer.position()];



                byteBuffer.put(putByte);
                byteBuffer.flip();
        try {
            reentrantLock.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        asynchronousFileChannel.write(byteBuffer, 0,new LockAndChannelProxy(reentrantLock,asynchronousFileChannel,byteBuffer), new CompletionHandler<Integer,LockAndChannelProxy>() {
                    @Override
                    public void completed(Integer result, LockAndChannelProxy attachment) {
                        try {
                            attachment.getAsynchronousFileChannel().close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        attachment.getByteBuffer().clear();
                        attachment.getReentrantLock().release();

                    }

                    @Override
                    public void failed(Throwable exc, LockAndChannelProxy attachment) {

                        try {
                            attachment.getAsynchronousFileChannel().close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        attachment.getByteBuffer().clear();
                        attachment.getReentrantLock().release();
                    }
                });




        }


    public void putMessage(DefaultBytesMessage message,KeyValue properties,DefaultProducer defaultProducer) {
        byte[] messageByte = serianized(message,properties);
     ByteBuffer byteBuffer = defaultProducer.getFlipByteBuffer(false);



        if (messageByte.length >= byteBuffer.remaining()) {



            byteBuffer.put(SendConstants.cutFlag);

            sendMessage(byteBuffer,properties, defaultProducer.getReentrantLock());

            //defaultProducer.setByteBuffer(ByteBuffer.allocate(SendConstants.buffSize));

            byteBuffer = defaultProducer.getFlipByteBuffer(true);


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
    public  synchronized ByteBuffer deSerianied(KeyValue properties){
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        File file = new File(properties.getString("STORE_PATH") +"/"+atomicIntegerFileName.get());
        if (!file.exists()) {


            atomicBooleanOverFlag.compareAndSet(true,false);
            ByteBuffer result = getFlipBuffer(false);
            result.flip();

            semaphore.release();
            return result;
        }

        ByteBuffer resultBuffer = getFlipBuffer(false);
        ByteBuffer resultFlipBuffer = getFlipBuffer(true);
        resultFlipBuffer.clear();
        Path path = Paths.get(properties.getString("STORE_PATH") + "/" + atomicIntegerFileName.getAndAdd(1));
        AsynchronousFileChannel asynchronousFileChannel = null;
        try {
            asynchronousFileChannel = AsynchronousFileChannel.open(path, StandardOpenOption.READ);
        } catch (IOException e) {
            e.printStackTrace();
        }


        asynchronousFileChannel.read(resultFlipBuffer, 0, asynchronousFileChannel, new CompletionHandler<Integer, AsynchronousFileChannel>() {
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


       resultBuffer.flip();
        return resultBuffer;
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


    } byte[] lenFlag = preBuff.array();
            int len = 0;
            if (lenFlag[0] != 0) {
                int temp = lenFlag[0] * 255;
                len += temp;
            }
            len += lenFlag[1];


            int j=0;//j��ʾ�������ٸ��ֽ�
        byte[] lenFlag=new byte[2];
        if(length>255){
             j=length/255;
        }

        lenFlag[0]= (byte) j;
        lenFlag[1]= (byte) length;

*/
    public   void insertMessage(ByteBuffer byteBuffer) {



        //byteBuffer.flip();
       // System.out.println(buffBytes.length);
//        for (int k = 0;k < 200;k++) {
//            System.out.print(buffBytes[k]);
//
//        }



        int headerNum = 0;
        int propertiesNum = 0;
        int indexNum = 0;
        int headerKeyLen = 0;
        int headerValueLen = 0;
        int propertiesKeyLen = 0;
        int propertiesValueLen = 0;
        DefaultBytesMessage defaultBytesMessage = new DefaultBytesMessage(null);
        while (true) {
            headerNum = byteBuffer.get();
            propertiesNum = byteBuffer.get();

            for (int headerIndex = 0; headerIndex < headerNum; headerIndex++) {
                byte len0 = byteBuffer.get();
                int headerVLen = 0;
                if (len0 != 0) {
                    int temp = len0 * 16129;

                    headerVLen += temp;
                }

                byte len1 = byteBuffer.get();
                if (len1 != 0) {
                    int temp = len1 * 127;

                    headerVLen += temp;
                }



                    headerVLen += byteBuffer.get();




                byte[] headerValueByte = new byte[headerVLen];
                for (int headerValueIndex = 0; headerValueIndex < headerVLen; headerValueIndex++) {
                    headerValueByte[headerValueIndex] = byteBuffer.get();

                }


                defaultBytesMessage.putHeaders(headerStrings[headerIndex], new String(headerValueByte));
            }
            for (int propertiesIndex = 0; propertiesIndex < propertiesNum; propertiesIndex++) {


                byte len0 = byteBuffer.get();
                int propertiesVLen = 0;
                if (len0 != 0) {
                    int temp = len0 * 16129;

                    propertiesVLen += temp;
                }
                byte len1 = byteBuffer.get();
                if (len1 != 0) {
                    int temp = len1 * 127;

                    propertiesVLen += temp;
                }



                propertiesVLen += byteBuffer.get();

                byte[] propertiesValueByte = new byte[propertiesVLen];
                for (int propertiesValueIndex = 0; propertiesValueIndex < propertiesVLen; propertiesValueIndex++) {
                    propertiesValueByte[propertiesValueIndex] = byteBuffer.get();

                }

                defaultBytesMessage.putProperties(propertiesStrings[propertiesIndex], new String(propertiesValueByte));
            }


            byte len0 = byteBuffer.get();
            int bodyLen = 0;
            if (len0 != 0) {
                int temp = len0 * 16129;

                bodyLen += temp;
            }

            byte len1 = byteBuffer.get();

            if (len1 != 0) {
                int temp = len1 * 127;

                bodyLen+= temp;
            }



            bodyLen += byteBuffer.get();

            byte[] body2 = new byte[bodyLen];
            for (int indexBody = 0; indexBody < bodyLen; indexBody++) {
                body2[indexBody] = byteBuffer.get();

            }
            defaultBytesMessage.setBody(body2);





        /*    for (String s : defaultBytesMessage.headers().keySet()) {
                System.out.println(s + defaultBytesMessage.headers().getString(s));
            }

            for (String s : defaultBytesMessage.properties().keySet()) {
                System.out.println(s + defaultBytesMessage.properties().getString(s));
            }
            String body = new String(defaultBytesMessage.getBody());

            System.out.println(new String(body));

            System.out.println("++++++++++++++++++++++++++++");*/
            String bucket = defaultBytesMessage.headers().keySet().contains(MessageHeader.TOPIC) ? defaultBytesMessage.headers().getString(MessageHeader.TOPIC) : defaultBytesMessage.headers().getString(MessageHeader.QUEUE);

            List<Integer> list = threadIdMap.get(bucket);
            if (list == null) {
                list = new ArrayList<Integer>();
                threadIdMap.put(bucket, list);

            }
            Queue queue = null;

            for (int id : list) {

                queue = queueMap.get(id);
                queue.add(defaultBytesMessage);
            }


            if (indexNum<= byteBuffer.limit() - 2 && byteBuffer.get(byteBuffer.position()) != SendConstants.cutFlag) {
                defaultBytesMessage = new DefaultBytesMessage(null);
            }else {
                break;
            }

        }


        return ;
    }


    public  synchronized DefaultBytesMessage pullMessage(KeyValue properties,int threadId) {



        Queue<DefaultBytesMessage> defaultBytesMessagesQueue = queueMap.get(threadId);
        while (true) {


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

    public  void attachInit(Collection<String> topics,String queue,KeyValue properties,int threadId){





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

        if (flushFlag.compareAndSet(true,false)) {

            ByteBuffer byteBufferKey = ByteBuffer.allocate(SendConstants.buffSize);


            FileInputStream fileInputStreamKey = null;
            try {
                fileInputStreamKey = new FileInputStream(properties.getString("STORE_PATH")+"/"+"keys");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            FileChannel fileChannel1 = fileInputStreamKey.getChannel();
            try {
                fileChannel1.read(byteBufferKey);
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                fileChannel1.close();

                fileInputStreamKey.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            byte[] keyByte = byteBufferKey.array();

            int headerKeyNum = keyByte[0];
            int propertiesKeyNum = keyByte[1];
            int index = 2;
             headerStrings = new String[headerKeyNum];
             propertiesStrings = new String[propertiesKeyNum];

            for (int check = 0;check < headerStrings.length;check++) {

                int headerKeyLen = keyByte[index++];

                byte[] headerKey = new byte[headerKeyLen];
                for (int indexNum = 0;indexNum < headerKeyLen; indexNum++) {

                    headerKey[indexNum] = keyByte[index++];





                }
                headerStrings[check] = new String(headerKey);



            }

            for (int check = 0;check < propertiesStrings.length;check++) {

                int headerPropertiesLen = keyByte[index++];

                byte[] propertiesKey = new byte[headerPropertiesLen];
                for (int indexNum = 0;indexNum < headerPropertiesLen; indexNum++) {

                    propertiesKey[indexNum] = keyByte[index++];





                }
                propertiesStrings[check] = new String(propertiesKey);



            }








            FileInputStream fileInputStream = null;
            try {
                fileInputStream = new FileInputStream(properties.getString("STORE_PATH")+"/"+atomicIntegerFileName.getAndAdd(1));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            FileChannel fileChannel = fileInputStream.getChannel();
            try {
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


        }                               }



   public void flush(KeyValue properties,DefaultProducer defaultProducer) {
        ByteBuffer byteBuffer = defaultProducer.getFlipByteBuffer(false);

            File file = new File(properties.getString("STORE_PATH") + "/" + atomicIntegerFileName.getAndAdd(1));

            if (!file.exists()) {
                try {
                    file.createNewFile();
                 } catch (IOException e) {
                    e.printStackTrace();

                }
            }

            if (byteBuffer.hasRemaining()) {
                byteBuffer.put(SendConstants.cutFlag);
                byte[] putByte = new byte[byteBuffer.capacity() - byteBuffer.position()];
                byteBuffer.put(putByte);
                Path path = Paths.get(file.getAbsolutePath());
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

        }



    public AtomicInteger getAtomicIntegerThreadId() {
        return atomicIntegerThreadId;
    }

    public void setAtomicIntegerThreadId(AtomicInteger atomicIntegerThreadId) {
        this.atomicIntegerThreadId = atomicIntegerThreadId;
    }

    public ByteBuffer getFlipBuffer(boolean flag){
        if (flag == true) {
            if (resultBuffer == byteBuffer) {
                resultBuffer = byteBuffer2;
                return byteBuffer2;

            } else {
                resultBuffer = byteBuffer;
                return byteBuffer;
            }
        }else {
            if (resultBuffer == null) {
                resultBuffer = byteBuffer;

            }
            return resultBuffer;
        }
    }


    }
