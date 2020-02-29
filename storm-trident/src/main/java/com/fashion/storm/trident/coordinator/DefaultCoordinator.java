package com.fashion.storm.trident.coordinator;

import org.apache.storm.trident.spout.ITridentSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class DefaultCoordinator implements ITridentSpout.BatchCoordinator<Long> , Serializable {

    private static Logger Logger = LoggerFactory.getLogger(DefaultCoordinator.class);

    /**
     * 初始化的方法
     * @param l
     * @param aLong
     * @param x1
     * @return
     */
    public Long initializeTransaction(long l, Long aLong, Long x1) {
        Logger.info("initializing Transaction [ "+ l +"]");
        return null;
    }

    public void success(long l) {
        Logger.info("success Transaction [" + l+"] ");
    }

    public boolean isReady(long l) {
        return true;
    }


    /**
     * 关闭方法？
     */
    public void close() {


    }
}
