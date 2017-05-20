package io.openmessaging.demo;

import javax.imageio.stream.FileImageOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Created by E450C on 2017/4/24.
 */
public class FileChannelProxy {
    private FileChannel fileChannel;
    private int readFlag=0;
    private  boolean isEnd=false;
    private FileInputStream fileInputStream;
    private DefaultBytesMessage message=null;
    private FileOutputStream fileOutputStream;
    private ThreadLocal threadLocal=new ThreadLocal();
    // private AtomicBoolean lock=new AtomicBoolean(true);
    private Lock lock=new ReentrantLock();
    private ByteBuffer byteBuffer;
    public  RunThread runThread=new RunThread();



    public void run(){
        new Thread(runThread).start();
    }

    public int getReadFlag() {
        return readFlag;
    }

    public void setReadFlag(int readFlag) {
        this.readFlag = readFlag;
    }

    public boolean isEnd() {
        return isEnd;
    }

    public void setEnd(boolean end) {
        isEnd = end;
    }





    public FileChannel getFileChannel() {
        return fileChannel;
    }

    public void setFileChannel(FileChannel fileChannel) {
        this.fileChannel = fileChannel;
    }

    public FileInputStream getFileInputStream() {
        return fileInputStream;
    }

    public void setFileInputStream(FileInputStream fileInputStream) {
        this.fileInputStream = fileInputStream;
    }

    public DefaultBytesMessage getMessage() {
        return message;
    }

    public void setMessage(DefaultBytesMessage message) {
        this.message = message;
    }

    public FileOutputStream getFileOutputStream() {
        return fileOutputStream;
    }

    public void setFileOutputStream(FileOutputStream fileOutputStream) {
        this.fileOutputStream = fileOutputStream;
    }

    public Long getPosition() {

        return  threadLocal.get()==null?0: (Long) threadLocal.get();
    }

    public void setPosiLtion(Long position) {
        threadLocal.set(position);

    }

    public Lock getLock() {
        return lock;
    }

    public void setLock(Lock lock) {
        this.lock = lock;
    }

  /*  public AtomicBoolean getLock() {
        return lock;
    }

    public void setLock(AtomicBoolean lock) {
        this.lock = lock;
    }*/
}
class RunThread implements Runnable{
    //private ByteBuffer byteBuffer;
    private ByteBuffer[] byteBuffers=new ByteBuffer[2];
    private FileChannel fileChannel;
    private AtomicBoolean atomicBooleanRead=new AtomicBoolean(false);//messagestore通过 atomicBooleanwrite.compareAndSet(true,wirte)
    private AtomicBoolean atomicBooleanWrite=new AtomicBoolean(true);//                 atomicBooleanRead.set(true)与此线程同步
    private int buffSize;
    private int[] buffSizes=new int[2];
   // private ReentrantLock lock=new ReentrantLock();
    //private Condition signal=lock.newCondition();
    //private Semaphore semaphoreRead=new Semaphore(1);
   // private Semaphore semaphoreWrite=new Semaphore(1);
   // private Semaphore semaphoreRead2=new Semaphore(1);
   // private Semaphore getSemaphoreWrite2=new Semaphore(1);
    private Semaphore[] semaphoresReads={new Semaphore(1,true),new Semaphore(1,true)};
    private Semaphore[] semaphoresWrites={new Semaphore(1,true),new Semaphore(1,true)};
    private volatile int num=0;
    private volatile int runNum=0;
    @Override
    public void run() {
        while(true) {

            /*
        }
            while(!atomicBooleanRead.compareAndSet(true,false)){

            }*/
            int n=runNum%2;
            try {
                //long start=System.currentTimeMillis();
                semaphoresReads[n].acquire();
               // long end=System.currentTimeMillis();
               // System.out.println("等待put cust:"+(start-end));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
          /*  try {
                signal.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/

            //System.out.println("正在io");
            while (byteBuffers[n].hasRemaining()) {
                try {
                    fileChannel.write(byteBuffers[n]);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            //atomicBooleanWrite.set(true);

            //  System.out.println("io结束");
            //signal.notify();

            semaphoresWrites[n].release();
            ++runNum;
        }
        }


    public void setFileChannel(FileChannel fileChannel) {
        this.fileChannel = fileChannel;
    }
    public FileChannel getFileChannel(){
        return fileChannel;
    }

   /* public void setByteBuffer(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }
    public ByteBuffer getByteBuffer(){
        return byteBuffer;
    }*/
    public AtomicBoolean getAtomicBooleanRead(){
        return atomicBooleanRead;
    }
    public AtomicBoolean getAtomicBooleanWrite(){
        return atomicBooleanWrite;
    }

    public int getBuffSize() {
        return buffSize;
    }

    public void setBuffSize(int buffSize) {
        this.buffSize = buffSize;
    }
   /* public void setLock(ReentrantLock lock){
        this.lock=lock;

    }
    public ReentrantLock getLock(){
        return lock;
    }
    public void setSignal(Condition condition){
        this.signal=condition;
    }
    public Condition getSignal(){
        return signal;
    }

    public Semaphore getSemaphoreWrite() {
        return semaphoreWrite;
    }

    public void setSemaphoreWrite(Semaphore semaphoreWrite) {
        this.semaphoreWrite = semaphoreWrite;
    }

    public Semaphore getSemaphoreRead() {
        return semaphoreRead;
    }

    public void setSemaphoreRead(Semaphore semaphoreRead) {
        this.semaphoreRead = semaphoreRead;
    }
*/
    public int getIsFirst() {
        return num;
    }

    public void setIsFirst(int isFirst) {
        this.num = isFirst;
    }
    public void add(){
        ++this.num;
    }

    public ByteBuffer[] getByteBuffers() {
        return byteBuffers;
    }

    public void setByteBuffers(ByteBuffer[] byteBuffers) {
        this.byteBuffers = byteBuffers;
    }

   /* public Semaphore getSemaphoreRead2() {
        return semaphoreRead2;
    }

    public void setSemaphoreRead2(Semaphore semaphoreRead2) {
        this.semaphoreRead2 = semaphoreRead2;
    }

    public Semaphore getGetSemaphoreWrite2() {
        return getSemaphoreWrite2;
    }

    public void setGetSemaphoreWrite2(Semaphore getSemaphoreWrite2) {
        this.getSemaphoreWrite2 = getSemaphoreWrite2;
    }*/

    public Semaphore[] getSemaphoresReads() {
        return semaphoresReads;
    }

    public void setSemaphoresReads(Semaphore[] semaphoresReads) {
        this.semaphoresReads = semaphoresReads;
    }

    public Semaphore[] getSemaphoresWrites() {
        return semaphoresWrites;
    }

    public void setSemaphoresWrites(Semaphore[] semaphoresWrites) {
        this.semaphoresWrites = semaphoresWrites;
    }

    public int[] getBuffSizes() {
        return buffSizes;
    }

    public void setBuffSizes(int[] buffSizes) {
        this.buffSizes = buffSizes;
    }
}
