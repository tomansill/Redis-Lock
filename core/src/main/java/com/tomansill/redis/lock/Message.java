package com.tomansill.redis.lock;

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

	private Message(final Type type, final String client_id, final String lock_id, final String lockpoint, final long lease_time){
		this.type = type;
		this.client_id = client_id;
		this.lock_id = lock_id;
		this.lockpoint = lockpoint;
		this.lease_time = lease_time;
	}

	static Message interpret(final String message){
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

			// Check the semicolon after type
			if(message.charAt(1) != ':') return null;

			// If FREE, OPEN, or SHARED message, then scan all and return message
			if(type == Type.FREE || type == Type.SHARED || type == Type.OPEN){
				if(message.indexOf(":", 2) != -1) return null;
				return new Message(type, null, null, message.substring(3), -1);
			}

			// Get first semicolon
			int first_semi = message.indexOf(":", 2);

			// Client id
			String client_id = message.substring(3, first_semi);

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
}
