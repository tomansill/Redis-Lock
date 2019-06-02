package com.ansill.redis.lock.exception;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ScriptErrorException extends RuntimeException{

	public ScriptErrorException(final String script_name, final String script, final ScriptHashErrorException exception){
		super(buildMessage(script_name, script, exception));
	}

	private static String buildMessage(final String script_name, final String script, final ScriptHashErrorException exception){

		// String builder
		StringBuilder message = new StringBuilder();

		// Header
		message.append("Syntax error in script hash: ");
		message.append(exception.getScriptHash());
		message.append("\nReason: ");
		message.append(exception.getReason());
		message.append('\n');

		// If it exists, build the whole script
		if(script != null){

			// ### script ###
			message.append(IntStream.range(0, 30).mapToObj(x -> "=").collect(Collectors.joining()));
			message.append(' ');
			message.append(script_name);
			message.append(' ');
			message.append(IntStream.range(0, 30).mapToObj(x -> "=").collect(Collectors.joining()));
			message.append('\n');

			// Script
			int count = 0;
			int pad_len = (exception.getLineNumber() + "").length();
			for(String line : script.split("\n")){
				count++;
				message.append("| ");
				message.append(String.format("%1$" + pad_len + "s", count));
				if(exception.getLineNumber() == count){
					message.append(" >> ");
					message.append(line);
					message.append('\n');
					message.append(IntStream.range(0, pad_len + 5).mapToObj(x -> ">").collect(Collectors.joining()));
					message.append("  ");
					message.append(exception.getReason());
				}else{
					message.append(" |  ");
					message.append(line);
				}
				message.append('\n');
			}

			// End with ######
			message.append(IntStream.range(0, 62 + script_name.length())
					               .mapToObj(x -> "=")
					               .collect(Collectors.joining()));
		}else{
			message.append("Failed to load script");
		}

		// Return it
		return message.toString();
	}
}
