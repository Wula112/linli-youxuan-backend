package com.hmdp.framework.lock;

public interface ILock {

    boolean tryLock(Long timeSeconds);

    void unlock();
}
