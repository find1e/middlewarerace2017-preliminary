package io.openmessaging.demo;

import io.openmessaging.*;

import java.util.*;

public class DemoTester {


    public static void main(String[] args) {
        KeyValue properties = new DefaultKeyValue();
        /*
        //实际测试时利用 STORE_PATH 传入存储路径
        //所有producer和consumer的STORE_PATH都是一样的，选手可以自由在该路径下创建文件
         */
        properties.put("STORE_PATH", "/home/fbhw/race");
        //properties.put("STORE_PATH", "D:\\race");

        //这个测试程序的测试逻辑与实际评测相似，但注意这里是单线程的，实际测试时会是多线程的，并且发送完之后会Kill进程，再起消费逻辑

        Producer producer = new DefaultProducer(properties);

        //构造测试数据
        String topic1 = "TOPIC_1"; //实际测试时大概会有100个Topic左右
        String topic2 = "TOPIC_2"; //实际测试时大概会有100个Topic左右
        //String queue1 = "QUEUE_1"; //实际测试时，queue数目与消费线程数目相同
        String queue2 = "QUEUE_2"; //实际测试时，queue数目与消费线程数目相同
        List<DefaultBytesMessage> messagesForTopic1 = new ArrayList<>(1024);
        List<DefaultBytesMessage> messagesForTopic2 = new ArrayList<>(1024);
      //  List<DefaultBytesMessage> messagesForQueue1 = new ArrayList<>(1024);
        List<DefaultBytesMessage> messagesForQueue2 = new ArrayList<>(1024);
        for (int i = 0; i < 1024; i++) {
            //注意实际比赛可能还会向消息的headers或者properties里面填充其它内容
            DefaultBytesMessage defaultBytesMessage = (DefaultBytesMessage) producer.createBytesMessageToTopic(topic1, (topic1 + i).getBytes());
            defaultBytesMessage.putHeaders("id","hello");

            defaultBytesMessage.putProperties("id"," are you ok");
            messagesForTopic1.add(defaultBytesMessage);
            DefaultBytesMessage defaultBytesMessage1 = (DefaultBytesMessage) producer.createBytesMessageToTopic(topic2, (topic2 + i).getBytes());
            defaultBytesMessage1.putHeaders("id","hello");
            defaultBytesMessage1.putProperties("id"," are you ok");
            messagesForTopic2.add(defaultBytesMessage1);

          /*  DefaultBytesMessage defaultBytesMessage2 =(DefaultBytesMessage) producer.createBytesMessageToQueue(queue1, (queue1 + i).getBytes());
            defaultBytesMessage2.putHeaders("id","hello");

            defaultBytesMessage.putProperties("id"," are you ok");
            messagesForQueue1.add(defaultBytesMessage2);
*/
            DefaultBytesMessage defaultBytesMessage3 =(DefaultBytesMessage) producer.createBytesMessageToQueue(queue2, (queue2 + i).getBytes());
            defaultBytesMessage3.putHeaders("id","hello");

            defaultBytesMessage3.putProperties("id"," are you ok");
            messagesForQueue2.add(defaultBytesMessage3); }

        long start = System.currentTimeMillis();
        //发送, 实际测试时，会用多线程来发送, 每个线程发送自己的Topic和Queue
        for (int i = 0; i < 1024; i++) {
            producer.send(messagesForTopic1.get(i));
            producer.send(messagesForTopic2.get(i));
        //    producer.send(messagesForQueue1.get(i));
            producer.send(messagesForQueue2.get(i));

        }
        long end = System.currentTimeMillis();

        long T1 = end - start;
        producer.flush();

        //请保证数据写入磁盘中

        {
            PullConsumer consumer2 = new DefaultPullConsumer(properties);
            List<String> topics = new ArrayList<>();
            topics.add(topic1);
            topics.add(topic2);
            consumer2.attachQueue(queue2, topics);
            int queue2Offset = 0, topic1Offset = 0, topic2Offset = 0;

            long startConsumer = System.currentTimeMillis();
            while (true) {
                DefaultBytesMessage message = (DefaultBytesMessage) consumer2.poll();
                if (message == null) {
                    //拉取为null则认为消息已经拉取完毕
                    break;
                }

                String topic = message.headers().getString(MessageHeader.TOPIC);
                String queue = message.headers().getString(MessageHeader.QUEUE);
                //实际测试时，会一一比较各个字段
                //实际测试时，会一一比较各个字段
                if (topic != null) {
                    if (topic.equals(topic1)) {
                        DefaultBytesMessage defaultBytesMessage = messagesForTopic1.get(topic1Offset++);

                       // System.out.println(defaultBytesMessage.headers().getString("id"));

                        System.out.println("topic1"+compare(defaultBytesMessage, message));
                    } else {
                        //Assert.assertEquals(topic2, topic);
                        DefaultBytesMessage defaultBytesMessage = messagesForTopic2.get(topic2Offset++);

                        //System.out.println(defaultBytesMessage.headers().getString("id"));

                        System.out.println("topic2"+compare(defaultBytesMessage, message));

                    }
                } else {
                    DefaultBytesMessage defaultBytesMessage = messagesForQueue2.get(queue2Offset++);


                   // System.out.println(defaultBytesMessage.headers().getString("id") );

                    System.out.println("queue2"+compare(defaultBytesMessage, message));

                    //Assert.assertEquals(queue2, queue);
                }
            }
            long endConsumer = System.currentTimeMillis();
            long T2 = endConsumer - startConsumer;
            System.out.println(String.format("Team2 cost:%d ms tps:%d q/ms", T2 + T1, (queue2Offset + topic1Offset)/(T1 + T2)));
        }


        //消费样例1，实际测试时会Kill掉发送进程，另取进程进行消费
      /*  {
            PullConsumer consumer1 = new DefaultPullConsumer(properties);
            consumer1.attachQueue(queue1, Collections.singletonList(topic1));

            int queue1Offset = 0, topic1Offset = 0;

            long startConsumer = System.currentTimeMillis();
            while (true) {

                DefaultBytesMessage message = (DefaultBytesMessage) consumer1.poll();
                if (message == null) {
                    //拉取为null则认为消息已经拉取完毕
                    break;
                }
                String topic = message.headers().getString(MessageHeader.TOPIC);
                String queue = message.headers().getString(MessageHeader.QUEUE);
                //实际测试时，会一一比较各个字段
                if (topic != null) {
                    //Assert.assertEquals(topic1, topic);
                   // DefaultBytesMessage defaultBytesMessage = messagesForTopic1.get(topic1Offset++);
                   // System.out.println(message.headers().getString(message.headers().keySet().iterator().next())+"----"+defaultBytesMessage.headers().getString(defaultBytesMessage.headers().keySet().iterator().next()));

                   // System.out.print(Arrays.toString(message.getBody()));
                    //System.out.println(Arrays.toString(defaultBytesMessage.getBody()));
                    System.out.println("topic1"+compare(messagesForTopic1.get(topic1Offset++), message));

                } else {

                  //  DefaultBytesMessage defaultBytesMessage = messagesForQueue1.get(queue1Offset++);
                   // System.out.println(message.headers().getString(message.headers().keySet().iterator().next())+"----"+defaultBytesMessage.headers().getString(defaultBytesMessage.headers().keySet().iterator().next()));
                    //System.out.print(Arrays.toString(message.getBody()));
                    //System.out.println(Arrays.toString(defaultBytesMessage.getBody()));
                    //Assert.assertEquals(queue1, queue);
                   System.out.println("queue1"+compare(messagesForQueue1.get(queue1Offset++), message));
                }
            }
            long endConsumer = System.currentTimeMillis();
            long T2 = endConsumer - startConsumer;
            System.out.println(String.format("Team1 cost:%d ms tps:%d q/ms", T2 + T1, (queue1Offset + topic1Offset)/(T1 + T2)));

        }*/
        //消费样例2，实际测试时会Kill掉发送进程，另取进程进行消费

    }
    public static boolean compare(DefaultBytesMessage defaultBytesMessage1, DefaultBytesMessage defaultBytesMessage2) {



        byte[] body = defaultBytesMessage1.getBody();



        byte[] body2 = defaultBytesMessage2.getBody();

        for(int indexNum=0;indexNum<body.length;indexNum++){
            if(body[indexNum]!=body2[indexNum]){
                return false;

            }
        }


       Iterator iterator = defaultBytesMessage1.headers().keySet().iterator();
            Iterator iterator1 = defaultBytesMessage2.headers().keySet().iterator();

            while (iterator.hasNext() || iterator1.hasNext()) {
                String s1 = (String) iterator.next();
                String s2 = (String) iterator1.next();
                if (!s1.equals(s2)) {
                    return false;

                }
                if (!defaultBytesMessage1.headers().getString(s1).equals(defaultBytesMessage2.headers().getString(s2)))

                {
                    return false;
                }


        }

        Iterator i = defaultBytesMessage1.properties().keySet().iterator();
        Iterator i1 = defaultBytesMessage2.properties().keySet().iterator();

        while (i.hasNext() || i1.hasNext()) {
            String s1 = (String) i.next();
            String s2 = (String) i1.next();
            if (!s1.equals(s2)) {
                return false;

            }
            if (!defaultBytesMessage1.properties().getString(s1).equals(defaultBytesMessage2.properties().getString(s2)))

            {
                return false;
            }


        }



        return true;
    }
    }



