
package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import io.openmessaging.PullConsumer;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DefaultPullConsumer implements PullConsumer {
    private MessageStore messageStore = MessageStore.getInstance();
    private KeyValue properties;
    private String queue;
    private Set<String> buckets = new HashSet<>();
    private List<String> bucketList = new ArrayList<>();


    // int remain;
    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
    }


    @Override public KeyValue properties() {
        return properties;
    }

    @Override public  Message poll() {
        return null;
       /* while (true){
            if(bucketList.size()==0){
                return null;

            }
            for (int checkNum=0;checkNum<bucketList.size();checkNum++) {
                MessageProxy messageProxy= messageStore.pullMessage(queue,bucketList.get(checkNum),properties);


                if(messageProxy.isEnd()){
                    bucketList.remove(checkNum);
                    break;
                }

                //  DefaultBytesMessage message=messageProxy.getDefaultBytesMessage();
                //   Lock lock=messageProxy.getLock();

                *//*if(lock!=null){
                    lock.unlock();
                    return message;

                }*//*

                return messageProxy.getDefaultBytesMessage();

            }


        }*/
    }
    @Override public Message poll(KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void ack(String messageId) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void ack(String messageId, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }
    @Override public  void attachQueue(String queueName, Collection<String> topics) {

        queue = queueName;

        buckets.addAll(topics);
        buckets.add(queueName);
        bucketList.clear();
        bucketList.addAll(buckets);
        messageStore.attachInit(bucketList,properties);


    }
}