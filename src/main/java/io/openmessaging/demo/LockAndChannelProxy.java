package io.openmessaging.demo;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by fbhw on 17-6-2.
 */
public class LockAndChannelProxy {
    private Semaphore reentrantLock = null;

    private AsynchronousFileChannel asynchronousFileChannel = null;

    private ByteBuffer byteBuffer = null;
    public LockAndChannelProxy(Semaphore reentrantLock,AsynchronousFileChannel asynchronousFileChannel,ByteBuffer byteBuffer){

        this.reentrantLock = reentrantLock;
        this.asynchronousFileChannel = asynchronousFileChannel;

        this.byteBuffer = byteBuffer;
    }
    public Semaphore getReentrantLock() {
        return reentrantLock;
    }

    public void setReentrantLock(Semaphore reentrantLock) {
        this.reentrantLock = reentrantLock;
    }

    public AsynchronousFileChannel getAsynchronousFileChannel() {
        return asynchronousFileChannel;
    }

    public void setAsynchronousFileChannel(AsynchronousFileChannel asynchronousFileChannel) {
        this.asynchronousFileChannel = asynchronousFileChannel;
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    public void setByteBuffer(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }
}
