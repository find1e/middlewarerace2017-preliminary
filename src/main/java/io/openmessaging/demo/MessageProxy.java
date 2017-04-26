package io.openmessaging.demo;

import io.openmessaging.Message;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by E450C on 2017/4/23.
 */
public class MessageProxy {
    private DefaultBytesMessage defaultBytesMessage;
    private boolean isEnd=false;

public MessageProxy(DefaultBytesMessage defaultBytesMessage){
    this.defaultBytesMessage=defaultBytesMessage;

}
public MessageProxy(Boolean isEnd){
    this.isEnd=isEnd;
}

public MessageProxy(DefaultBytesMessage defaultBytesMessage,boolean isEnd){
    this.isEnd=isEnd;
    this.defaultBytesMessage=defaultBytesMessage;

}





    public DefaultBytesMessage getDefaultBytesMessage() {
        return defaultBytesMessage;
    }

    public void setDefaultBytesMessage(DefaultBytesMessage defaultBytesMessage) {
        this.defaultBytesMessage = defaultBytesMessage;
    }

    public boolean isEnd() {
        return isEnd;
    }

    public void setEnd(boolean end) {
        isEnd = end;
    }
}
