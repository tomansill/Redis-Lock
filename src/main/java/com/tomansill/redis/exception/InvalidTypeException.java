package com.tomansill.redis.exception;

public class InvalidTypeException extends RuntimeException{
	public InvalidTypeException(final String expected, final String actual){
		super("Invalid type! Expected " + expected + " Actual: " + actual);
	}

	public InvalidTypeException(final String expected, final String actual, final Throwable throwable){
		super("Invalid type! Expected " + expected + " Actual: " + actual, throwable);
	}

	public InvalidTypeException(final Throwable throwable){
		super(throwable);
	}
}
