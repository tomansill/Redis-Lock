package com.tomansill.redis.exception;

public class ScriptHashErrorException extends Exception{

	private final String script_hash;

	private final String reason;

	private int line_number = -1;

	public ScriptHashErrorException(String message, String script_hash, int line_number){
		super("Script hash: '" + script_hash + "' \tline number: " + line_number + "\tmessage: " + message);
		this.script_hash = script_hash;
		this.line_number = line_number;
		this.reason = message;
	}

	public String getScriptHash(){
		return this.script_hash;
	}

	public int getLineNumber(){
		return this.line_number;
	}

	public String getReason(){
		return this.reason;
	}
}
