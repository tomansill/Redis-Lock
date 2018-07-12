package com.tomansill.redis;

public interface RedisClient{

    public boolean isCluster() throws IOException;

    public boolean Collection<String> getMasterNodes() throws IOException;

    public String get(String key) throws IOException;

    public String set(String key, String value) throws IOException;

    public String setnx(String key, String value) throws IOException;

    public void expire(String key, long ttl) throws IOException;
}
