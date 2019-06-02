package com.ansill.redis.test;

import javax.annotation.Nonnull;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class TestUtility{

    private TestUtility(){
    }

    @SuppressWarnings("unchecked")
    public static <T> T assertType(@Nonnull Class<T> clazz, @Nonnull Object object, @Nonnull String message){
        assertEquals(clazz, object.getClass(), message);
        return (T) object;
    }

    @SuppressWarnings("unchecked")
    public static <T> T assertType(@Nonnull Class<T> clazz, @Nonnull Object object){
        assertEquals(clazz, object.getClass());
        return (T) object;
    }
}
