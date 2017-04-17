package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;

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

public class MessageStore {

    private static final MessageStore INSTANCE = new MessageStore();

    public static MessageStore getInstance() {
        return INSTANCE;
    }

    private Map<String, ArrayList<Message>> messageBuckets = new HashMap<>();

    private Map<String, HashMap<String, Integer>> queueOffsets = new HashMap<>();

    private Lock lockDirect=new ReentrantLock();

    private Lock lockFile=new ReentrantLock();


    public void putMessage(String bucket, Message message,KeyValue properties) throws IOException {

        File fileChild = new File(properties.getString("STORE_PATH") + "/" + bucket);
        lockDirect.lock();
        if (!fileChild.exists()) {

            fileChild.createNewFile();
        }
        lockDirect.unlock();

        FileOutputStream fileOutputStream = new FileOutputStream(fileChild);

        FileChannel fileChannel = fileOutputStream.getChannel();


        DefaultBytesMessage defaultBytesMessage = (DefaultBytesMessage) message;

        ByteBuffer byteBuffer = ByteBuffer.allocate(defaultBytesMessage.getBody().length);

        byteBuffer.put(defaultBytesMessage.getBody());
        lockFile.lock();
        fileChannel.write(byteBuffer);

        lockFile.unlock();
        fileChannel.close();
        fileOutputStream.close();
    }







    public synchronized Message pullMessage(String queue, String bucket) {
        ArrayList<Message> bucketList = messageBuckets.get(bucket);
        if (bucketList == null) {
            return null;
        }
        HashMap<String, Integer> offsetMap = queueOffsets.get(queue);
        if (offsetMap == null) {
            offsetMap = new HashMap<>();
            queueOffsets.put(queue, offsetMap);
        }
        int offset = offsetMap.getOrDefault(bucket, 0);
        if (offset >= bucketList.size()) {
            return null;
        }
        Message message = bucketList.get(offset);
        offsetMap.put(bucket, ++offset);
        return message;
   }
}
