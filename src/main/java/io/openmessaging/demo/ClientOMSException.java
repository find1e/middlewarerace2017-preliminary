package io.openmessaging.demo;

import io.openmessaging.exception.OMSException;
import io.openmessaging.exception.OMSResourceNotExistException;
import io.openmessaging.exception.OMSRuntimeException;

import java.nio.channels.Channel;
import java.nio.charset.CharacterCodingException;

class ClientOMSException extends OMSRuntimeException {

    public String message;
    public ClientOMSException(String message) {
        this.message = message;
    }
    public ClientOMSException(String message, Throwable throwable) {
        this.initCause(throwable);
        this.message = message;
    }

    @Override
    public String getMessage() {
        return message;
    }



}

