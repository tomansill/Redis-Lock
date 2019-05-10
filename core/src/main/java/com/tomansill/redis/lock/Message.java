package com.tomansill.redis.lock;

import java.util.Arrays;

class Message{

	/** enum **/
	enum Type{
		LOCK,
		UNLOCK,
		SHARED,
		OPEN,
		FREE
	}

	final Type type;

	final String client_id;

	final String lock_id;

	public final String lockpoint;

	final long lease_time;

	private Message(Type type, String client_id, String lock_id, String lockpoint, long lease_time) {
		this.type = type;
		this.client_id = client_id;
		this.lock_id = lock_id;
		this.lockpoint = lockpoint;
		this.lease_time = lease_time;
	}

	static Message interpret(String message) {

		System.out.println("interpret(message=" + (message == null ? "null" : "'" + message + "'") + ")");

		// Ignore if empty
		if (message == null || message.trim().isEmpty()) return null;

		// Split string
		String[] tokens = message.trim().split(":");

		System.out.println("tokenized: " + Arrays.toString(tokens));

		// Switch on token length
		if(tokens.length == 2){

			// Most likely FREE, OPEN, SHARED, ... or invalid message, find out which one
			Type type;
			switch(tokens[0].trim()){
				case "#":
					type = Type.FREE;
					break;
				case "o":
					type = Type.OPEN;
					break;
				case "s":
					type = Type.SHARED;
					break;
				default: return null; // Invalid message
			}

			// Return the message
			return new Message(type, null, null, tokens[1].trim(), -1);

		}else if(tokens.length == 3){

			// Check if correct tag
			if(!tokens[0].trim().equals("u")) return null;

			// Return message
			return new Message(Type.UNLOCK, null, null, tokens[2].trim(), -1);

		}else if(tokens.length == 5){

			// Check if correct tag
			if(!tokens[0].trim().equals("l")) return null;

			try{
				return new Message(Type.LOCK, tokens[1].trim(), tokens[2].trim(), tokens[4].trim(), Long.parseLong(tokens[3]));
			}catch(NumberFormatException ignored){
				return null;
			}
		}

		// If arrived here, then the message is invalid
		return null;
	}

	static Message interpret1(String message) {
		if(message == null || message.isEmpty()) return null;

		try{
			// Get type
			Type type;
			if(message.charAt(0) == 'l') type = Type.LOCK;
			else if(message.charAt(0) == 'u') type = Type.UNLOCK;
			else if(message.charAt(0) == 's') type = Type.SHARED;
			else if(message.charAt(0) == 'o') type = Type.OPEN;
			else if(message.charAt(0) == '#') type = Type.FREE;
			else return null;

			System.out.println("type=" + type);

			// Check the semicolon after type
			if(message.charAt(1) != ':') return null;

			// If FREE, OPEN, or SHARED message, then scan all and return message
			if(type == Type.FREE || type == Type.SHARED || type == Type.OPEN){
				if(message.indexOf(":", 2) != -1) return null;
				return new Message(type, null, null, message.substring(2), -1);
			}

			// Get first semicolon
			int first_semi = message.indexOf(":", 2);

			// Client id
			String client_id = message.substring(2, first_semi);

			// Get second semicolon
			int second_semi = message.indexOf(":", first_semi + 1);

			// lock id
			String lock_id = message.substring(first_semi + 1, second_semi);

			// Get third semicolon
			int third_semi = message.indexOf(":", second_semi + 1);

			// Enough info for UNLOCK Message
			if(type == Type.UNLOCK){
				if(third_semi != -1) return null;
				return new Message(type, client_id, lock_id, message.substring(second_semi + 1), -1);
			}

			// lock lease time
			long lock_lease_time = Long.parseLong(message.substring(second_semi + 1, third_semi));

			// lockpoint
			String lockpoint = message.substring(third_semi + 1);

			// Check if there's no extra stuff
			if(message.indexOf(":", second_semi + 1) != -1) return null;

			// Return it
			return new Message(type, client_id, lock_id, lockpoint, lock_lease_time);

		}catch(IndexOutOfBoundsException | NumberFormatException ignored){
			return null;
		}
	}

	public String toString(){
		return "Message(type=" + type + ", client_id=" + client_id + ", lock_id=" + lock_id + ", lockpoint=" + lockpoint + ", lease_time=" + lease_time + ")";
	}
}
