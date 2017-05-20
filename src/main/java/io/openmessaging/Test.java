package io.openmessaging;

import sun.nio.cs.ext.PCK;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousByteChannel;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by E450C on 2017/5/2.
 */
public class Test {
    public static void main(String[] args) throws InterruptedException, IOException {
        Path path = Paths.get("D:\\race\\test.txt");
        AsynchronousFileChannel asynchronousFileChannel = AsynchronousFileChannel.open(path,StandardOpenOption.WRITE);



            final ByteBuffer buffer = ByteBuffer.allocate(20);
        buffer.put("888".getBytes());
        buffer.flip();

            asynchronousFileChannel.write(buffer, 100, "attachment information",
                    new CompletionHandler<Integer, Object>() {
                        @Override
                        public void completed(Integer readCount, Object attachment) {
                            System.out.println(readCount);
                            System.out.println(attachment);
                        /*try {
                            Thread.sleep(5000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }*/
                            System.out.println(new String(buffer.array()));
                        }

                        @Override
                        public void failed(Throwable exc, Object attachment) {
                            System.out.println("Error:" + exc);
                        }
                    });
            System.out.println("continue doing other things");
            // Thread.sleep(1000);



    }}





