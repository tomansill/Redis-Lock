package com.ansill.com.redis.script.lock;

import com.ansill.redis.lock.Utility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class UtilityTest{

    @Test
    void format(){

        Assertions.assertEquals("Hello my name is Tom", Utility.format("Hello my name is {}", "Tom"));

        assertEquals("Hello my name is Tom {} {}", Utility.format("Hello my name is {} {} {}", "Tom"));

        assertEquals("Hello my name is {}", Utility.format("Hello my name is {}"));


    }
}