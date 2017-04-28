package io.openmessaging.demo;

import io.openmessaging.StreamCallBack;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by E450C on 2017/4/21.
 */
class DistributeLock  {
    private Node[] nodes;
    private int size;
    AtomicBoolean atomic = new AtomicBoolean(true);


    public DistributeLock(int init) {
        size = init;
        nodes = new Node[init];
    }
    static final int hash(Object key) {
        int h;
        return (key == null) ? 0 : (h = key.hashCode()) ^ (h >>> 16);
    }

    public FileChannelProxy lock(String bucked, StreamCallBack callBack){
        if (!atomic.compareAndSet(true, false)) {
            return null;
        }
        FileChannelProxy fileChannelProxy=null;
     Node node=get(bucked);


        if(node==null){

            fileChannelProxy=callBack.callBack();
           node=put(bucked,fileChannelProxy);
        }

         fileChannelProxy=node.getValue();

        AtomicBoolean lock=fileChannelProxy.getLock();
        while(!lock.compareAndSet(true, false));
        atomic.set(true);
        return fileChannelProxy;
    }

    public void unLock(FileChannelProxy fileChannelProxy){
       AtomicBoolean lock=fileChannelProxy.getLock();
        lock.set(true);
    }

    public   Node put(String key,FileChannelProxy fileChannelProxy) {

        int hash=size & hash(key);
        Node node = new Node(key,fileChannelProxy, true);
        if(nodes[hash]==null){
            nodes[hash]=node;
            return node;
        }

        Node preNode=nodes[hash];
        Node temp=null;
        while ((temp=preNode.getNext())!=null) {
            preNode=temp;
            if (key.equals(temp.getKey())) {
               temp.setValue(fileChannelProxy);
               return node;
            }

        }

        node.setNext(temp);
        preNode.setNext(node);
        return node;
    }

    public void remove(String key) {

    }
    public Node get(String key) {

        Node local = nodes[size & hash(key)];
        if (local == null) {
            return null;
        }
        if (key.equals(local.getKey())) {
            return local;
        }
        Node node=local;
        while ((node = node.getNext()) != null) {

            if (key.equals(node.getKey())) {
                return node;
            }
            }
        return null;
    }

    public static void main(String[] args) {
        DistributeLock distributeLock=new DistributeLock(200);
        distributeLock.put("hujunqiu",new FileChannelProxy());
        distributeLock.put("hujunqia",new FileChannelProxy());
        distributeLock.put("hujunqiw",new FileChannelProxy());
        distributeLock.put("hujunqie",new FileChannelProxy());
        distributeLock.put("hujunqir",new FileChannelProxy());
        System.out.println(distributeLock.get("hujunqiu"));
        System.out.println(distributeLock.get("hujunqia"));
        System.out.println(distributeLock.get("hujunqiw"));
        System.out.println(distributeLock.get("hujunqie"));
        System.out.println(distributeLock.get("hujunqir"));

        System.out.println(200 & hash("hujunqiu"));
        System.out.println(200 & hash("hujunqia"));
        System.out.println(200 & hash("hujunqiw"));
        System.out.println(200 & hash("hujunqie"));
        System.out.println(200 & hash("hujunqir"));


    }
}




