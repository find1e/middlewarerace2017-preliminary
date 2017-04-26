package io.openmessaging.demo;

import java.io.FileInputStream;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by E450C on 2017/4/24.
 */
public class FileChannelProxy {
    private FileChannel fileChannel;
    private int readFlag=0;
    private  boolean isEnd=false;
    private FileInputStream fileInputStream;
    private ReentrantLock lock=new ReentrantLock();
    private DefaultBytesMessage message=null;


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

    public ReentrantLock getLock() {
        return lock;
    }

    public void setLock(ReentrantLock lock) {
        this.lock = lock;
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
}
