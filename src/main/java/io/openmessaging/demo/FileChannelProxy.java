package io.openmessaging.demo;

import javax.imageio.stream.FileImageOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Created by E450C on 2017/4/24.
 */
public class FileChannelProxy {
    private FileChannel fileChannel;
    private int readFlag=0;
    private  boolean isEnd=false;
    private FileInputStream fileInputStream;
    private AtomicBoolean lock=new AtomicBoolean(true);
    private DefaultBytesMessage message=null;
    private FileOutputStream fileOutputStream;
    private ThreadLocal threadLocal=new ThreadLocal();


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

    public AtomicBoolean getLock() {
        return lock;
    }

    public void setLock(AtomicBoolean lock) {
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
}
