

package io.openmessaging.demo;

import io.openmessaging.*;
import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.Assert;

public class DemoTester {


    public static void main(String[] args) {
        final KeyValue properties = new DefaultKeyValue();
        /*
        //ʵ�ʲ���ʱ���� STORE_PATH ����洢·��
        //����producer��consumer��STORE_PATH����һ���ģ�ѡ�ֿ��������ڸ�·���´����ļ�
         */
        properties.put("STORE_PATH", "/home/admin/test");
        // properties.put("STORE_PATH", "/storage/sdcard0/AppProjects/alinative")
        //
        //
        //����Գ���Ĳ����߼���ʵ���������ƣ���ע�������ǵ��̵߳ģ�ʵ�ʲ���ʱ���Ƕ��̵߳ģ����ҷ�����֮���Kill���̣����������߼�
        final Producer producer = new DefaultProducer(properties);
        //�����������
        String topic1 = "TOPIC1"; //ʵ�ʲ���ʱ��Ż���100��Topic����
        String topic2 = "TOPIC2"; //ʵ�ʲ���ʱ��Ż���100��Topic����
        String queue1 = "QUEUE1"; //ʵ�ʲ���ʱ��Ż���100��Queue����
        String queue2 = "QUEUE2"; //ʵ�ʲ���ʱ��Ż���100��Queue����
        List<Message> messagesForTopic1 = new ArrayList<>(1024);
        List<Message> messagesForTopic2 = new ArrayList<>(1024);
        List<Message> messagesForQueue1 = new ArrayList<>(1024);
        List<Message> messagesForQueue2 = new ArrayList<>(1024);


        for (int i = 0; i < 1024; i++) {
            //ע��ʵ�ʱ������ܻ�������Ϣ��headers����properties���������������
            messagesForTopic1.add(producer.createBytesMessageToTopic(topic1, (topic1 + i).getBytes()));
            messagesForTopic2.add(producer.createBytesMessageToTopic(topic2, (topic2 + i).getBytes()));
            messagesForQueue1.add(producer.createBytesMessageToQueue(queue1, (queue1 + i).getBytes()));
            messagesForQueue2.add(producer.createBytesMessageToQueue(queue2, (queue2 + i).getBytes()));
        }
        long start = System.currentTimeMillis();
        //����, ʵ�ʲ���ʱ�����ö��߳�������, ÿ���̷߳����Լ���Topic��Queue
        for (int i = 0; i < 1024; i++) {

            producer.send(messagesForTopic1.get(i));
            producer.send(messagesForTopic2.get(i));
            producer.send(messagesForQueue1.get(i));
            producer.send(messagesForQueue2.get(i));
        }
  /*    new Thread(new Runnable() {
           @Override
           public void run() {
               for (int i = 0; i < 1024; i++) {
                   KeyValue properties = new DefaultKeyValue();
                   properties.put("STORE_PATH", "D:\\race");
                   Producer producer = new DefaultProducer(properties);
 producer.send(messagesForTopic1.get(i));

                }
            }
        }).start();
        new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 1024; i++) {
                    KeyValue properties = new DefaultKeyValue();
                    properties.put("STORE_PATH", "D:\\race");
                    Producer producer = new DefaultProducer(properties);
                    producer.send(messagesForTopic2.get(i));

                }
            }
        }).start();
        new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 1024; i++) {
                    KeyValue properties = new DefaultKeyValue();
                    properties.put("STORE_PATH", "D:\\race");
                    Producer producer = new DefaultProducer(properties);
                    producer.send(messagesForQueue1.get(i));
                }
            }
        }).start();
        new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 1024; i++) {
                    KeyValue properties = new DefaultKeyValue();
                    properties.put("STORE_PATH", "D:\\race");
                    Producer producer = new DefaultProducer(properties);
                    producer.send(messagesForQueue2.get(i));

                }
            }
        }).start();
*/


        long end = System.currentTimeMillis();

        long T1 = end - start;

        //�뱣֤����д�������
        ReentrantLock lock=new ReentrantLock();

        //��������1��ʵ�ʲ���ʱ��Kill�����ͽ��̣���ȡ���̽�������

            Task task1=new Task();
            Task2 task2=new Task2();
        task1.messagesForQueue1= (ArrayList) messagesForQueue1;
        task1.messagesForTopic1= (ArrayList) messagesForTopic1;
        task1.properties=properties;
        task1.queue1=queue1;
        task1.topic1=topic1;
        task2.topic1=topic1;
        task2.messagesForQueue2= (ArrayList) messagesForQueue2;
        task2.messagesForTopic1= (ArrayList) messagesForTopic1;
        task2.messagesForTopic2= (ArrayList) messagesForTopic2;
        task2.properties=properties;
        task2.topic2=topic2;
        task2.queue2=queue2;
       task1.lock=lock;
       task2.lock=lock;
        task1.T1=T1;
        task2.T1=T1;
 int topic10=0 ,topic20=0,queue10 = 0,queue20=0;
        task1.queue1Offset=queue10;
        task1.topic1Offset=topic10;
        task2.queue2Offset=queue20;
        task2.topic1Offset=topic10;
        task2.topic2Offset=topic20;

        Thread thread1=new Thread(task1);
        Thread thread2=new Thread(task2);

        //多线程环境下测试

          thread1.start();
          thread2.start();
    }

}
class Task2 implements Runnable{
    public String queue2;
    public String topic1;
    public String topic2;
    public KeyValue properties;
    public ArrayList messagesForTopic1;
    public ArrayList messagesForTopic2;
    public ArrayList messagesForQueue2;
    public ReentrantLock lock;
    public long T1;
    public int queue2Offset , topic1Offset , topic2Offset;
    @Override
    public void run() {


        PullConsumer consumer2 = new DefaultPullConsumer(properties);
        List<String> topics = new ArrayList<>();
        topics.add(topic1);
        topics.add(topic2);
        consumer2.attachQueue(queue2, topics);

      //  int queue2Offset = 0, /*topic1Offset = 0,*/ topic2Offset = 0;

        long startConsumer = System.currentTimeMillis();

        while (true) {

            Message message = consumer2.poll();

            lock.lock();
            if (message == null) {
                //��ȡΪnull����Ϊ��Ϣ�Ѿ���ȡ���
lock.unlock();
                break;
            }

            String topic = message.headers().getString(MessageHeader.TOPIC);
            String queue = message.headers().getString(MessageHeader.QUEUE);
               //ʵ�ʲ���ʱ����һһ�Ƚϸ����ֶ�
               if (topic != null) {

                if (topic.equals(topic1)) {

                   // DefaultBytesMessage b = (DefaultBytesMessage) messagesForTopic1.get(topic1Offset++);

                   // DefaultBytesMessage defaultBytesMessage = (DefaultBytesMessage) message;
                   //System.out.println("线程1topic1" + Arrays.toString(defaultBytesMessage.getBody()));
                   //System.out.println("线程1topic1" + Arrays.toString(b.getBody()) + "目标数组");
                   // Assert.assertEquals(topic1, topic);
                   //  Assert.assertEquals(messagesForTopic1.get(topic1Offset++), message);
                   // if(topic2Offset>=messagesForTopic2.size()){
                  //      continue;
                   // }
                   boolean compare=compare((DefaultBytesMessage) messagesForTopic2.get(topic2Offset++),(DefaultBytesMessage) message);
                    System.out.println("queue2"+compare);
                } else {

                   // Assert.assertEquals(topic2, topic);
                    //Assert.assertEquals(messagesForTopic2.get(topic2Offset++), message);
                 //   DefaultBytesMessage b = (DefaultBytesMessage) messagesForTopic2.get(topic2Offset++);

                 //   DefaultBytesMessage defaultBytesMessage = (DefaultBytesMessage) message;
                 //   System.out.println("1" + Arrays.toString(defaultBytesMessage.getBody()));
                 //  System.out.println("1" + Arrays.toString(b.getBody()) + "目标数组");
                  //  if(topic2Offset>=messagesForTopic2.size()){
//continue;
                 //   }
                   boolean compare=compare((DefaultBytesMessage) messagesForTopic2.get(topic2Offset++),(DefaultBytesMessage) message);
                    System.out.println("topic2"+compare);
                }

            } else {
                  // Assert.assertEquals(queue2, queue);
                   //Assert.assertEquals(messagesForQueue2.get(queue2Offset++), message);
              //  DefaultBytesMessage b = (DefaultBytesMessage) messagesForQueue2.get(queue2Offset++);

             //   DefaultBytesMessage defaultBytesMessage = (DefaultBytesMessage) message;
          //    System.out.println("1" + Arrays.toString(defaultBytesMessage.getBody()));
           //     System.out.println("1" + Arrays.toString(b.getBody()) + "目标数组");

                 //  if(queue2Offset>=messagesForQueue2.size()){
                 //      continue;
                 //  }
                boolean compare=compare((DefaultBytesMessage) messagesForQueue2.get(queue2Offset++),(DefaultBytesMessage) message);
           System.out.println("queue2"+compare);

            }
   lock.unlock();
        }


        long endConsumer = System.currentTimeMillis();
        long T2 = endConsumer - startConsumer;
        System.out.println(String.format("Team2 cost:%d ms tps:%d q/ms", T2+T1, (queue2Offset + topic1Offset) / (  T1+T2)));
    }






    public boolean compare(DefaultBytesMessage defaultBytesMessage1,DefaultBytesMessage defaultBytesMessage2) {
        KeyValue headers = defaultBytesMessage1.headers();
        String key= (String) headers.keySet().toArray()[0];
        String value=headers.getString(key);
        KeyValue properties = defaultBytesMessage1.properties();
        String key11 =(String) properties.keySet().toArray()[0];
        String value11=properties.getString(key11);
        byte[] body = defaultBytesMessage1.getBody();


        KeyValue headers2 = defaultBytesMessage2.headers();
        String key2= (String) headers2.keySet().toArray()[0];
        String value2=headers.getString(key);
        KeyValue properties2 = defaultBytesMessage2.properties();
        String key22 = (String) properties2.keySet().toArray()[0];
        String value22=properties2.getString(key22);
        byte[] body2 = defaultBytesMessage2.getBody();
        if(!key.equals(key2)){
            return false;
        }
        if(!key11.equals(key22)){
            return false;
        }
        if(!value.equals(value2)){
            return false;

        }
        if(!value11.equals(value22)){
            return false;

        }
        for(int indexNum=0;indexNum<body.length;indexNum++){
            if(body[indexNum]!=body2[indexNum]){
                return false;

            }
        }


        return true;
    }
}
class Task implements Runnable {

    public String queue1;
    public String topic1;
    public KeyValue properties;
    public ArrayList messagesForTopic1;
    public ArrayList messagesForQueue1;
    public ReentrantLock lock;
    public long T1;
    public int queue1Offset , topic1Offset ;
    public void run() {


        PullConsumer consumer1 = new DefaultPullConsumer(properties);
        consumer1.attachQueue(queue1, Collections.singletonList(topic1));



        long startConsumer = System.currentTimeMillis();

        while (true) {

            Message message = consumer1.poll();
   lock.lock();
            if (message == null) {
                //��ȡΪnull����Ϊ��Ϣ�Ѿ���ȡ���
                lock.unlock();
                break;
            }

            String topic = message.headers().getString(MessageHeader.TOPIC);
            String queue = message.headers().getString(MessageHeader.QUEUE);
                       //ʵ�ʲ���ʱ����һһ�Ƚϸ����ֶ�
            if (topic != null) {

             //   DefaultBytesMessage b = (DefaultBytesMessage) messagesForTopic1.get(topic1Offset++);

              //  DefaultBytesMessage defaultBytesMessage = (DefaultBytesMessage) message;
               // System.out.println("线程2topic1" + Arrays.toString(defaultBytesMessage.getBody()));
          //   System.out.println("线程2topic1" + Arrays.toString(b.getBody()) + "目标数组");

                //Assert.assertEquals(topic1, topic);
                   //  Assert.assertEquals(messagesForTopic1.get(topic1Offset++), message);
              //  if(topic1Offset>=messagesForTopic1.size()){
                //    continue;
             //   }
                boolean compare=compare((DefaultBytesMessage) messagesForTopic1.get(topic1Offset++),(DefaultBytesMessage) message);
               System.out.println("topic1"+compare);
            } else {


             // DefaultBytesMessage b = (DefaultBytesMessage) messagesForQueue1.get(queue1Offset++);

              //  DefaultBytesMessage defaultBytesMessage = (DefaultBytesMessage) message;
             //  System.out.println("2" + Arrays.toString(defaultBytesMessage.getBody()));
            //   System.out.println("2" + Arrays.toString(b.getBody()) + "目标数组");

                  // Assert.assertEquals(queue1, queue);
                 //Assert.assertEquals(messagesForQueue1.get(queue1Offset++), message);
              //  if(queue1Offset>=messagesForQueue1.size()){
              //      continue;
              //  }
                boolean compare=compare((DefaultBytesMessage) messagesForQueue1.get(queue1Offset++),(DefaultBytesMessage) message);
                System.out.println("queue1"+compare);
            }
   lock.unlock();
        }

        long endConsumer = System.currentTimeMillis();
        long T2 = endConsumer - startConsumer;

        System.out.println(String.format("Team1 cost:%d ms tps:%d q/ms", T2+T1, (queue1Offset + topic1Offset) / (T2+T1)));
    }
    public boolean compare(DefaultBytesMessage defaultBytesMessage1,DefaultBytesMessage defaultBytesMessage2) {
        KeyValue headers = defaultBytesMessage1.headers();
        String key= (String) headers.keySet().toArray()[0];
        String value=headers.getString(key);
        KeyValue properties = defaultBytesMessage1.properties();
        String key11 =(String) properties.keySet().toArray()[0];
        String value11=properties.getString(key11);
        byte[] body = defaultBytesMessage1.getBody();


        KeyValue headers2 = defaultBytesMessage2.headers();
        String key2= (String) headers2.keySet().toArray()[0];
        String value2=headers.getString(key);
        KeyValue properties2 = defaultBytesMessage2.properties();
        String key22 = (String) properties2.keySet().toArray()[0];
        String value22=properties2.getString(key22);
        byte[] body2 = defaultBytesMessage2.getBody();
       if(!key.equals(key2)){
            return false;
        }
       if(!key11.equals(key22)){
            return false;
        }
        if(!value.equals(value2)){
            return false;

        }
        if(!value11.equals(value22)){
            return false;

       }
       for(int indexNum=0;indexNum<body.length;indexNum++){
            if(body[indexNum]!=body2[indexNum]){
                return false;

            }
        }


        return true;
    }

}