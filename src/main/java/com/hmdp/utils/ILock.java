package com.hmdp.utils;

public interface ILock {

    boolean tryLock(Long timeSeconds);

    void unlock();
}
