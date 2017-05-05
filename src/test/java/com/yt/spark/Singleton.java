package com.yt.spark;

/**
 * 单例模式
 *
 * Created by yangtong on 17/5/5.
 */
public class Singleton {
    private static Singleton instance = null;

    //构造方法必须是private的
    private Singleton() {

    }

    public static Singleton getInstance() {
        if (instance == null) {
            synchronized (Singleton.class) {
                if (instance == null) {
                    instance = new Singleton();
                }
            }
        }
        return instance;
    }
}
