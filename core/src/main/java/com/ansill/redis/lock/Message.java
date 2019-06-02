package com.ansill.redis.lock;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;

public class Message{

    @Nonnull
    private final Type type;
    @Nullable
    private final String client_id;
    @Nullable
    private final String lock_id;
    @Nonnull
    private final String lockpoint;
    @Nullable
    private final Long lease_time;

    private Message(
            @Nonnull Type type,
            @Nullable String client_id,
            @Nullable String lock_id,
            @Nonnull String lockpoint,
            @Nullable Long lease_time
    ){
		this.type = type;
		this.client_id = client_id;
		this.lock_id = lock_id;
		this.lockpoint = lockpoint;
		this.lease_time = lease_time;
	}

    @Nonnull
    public static Optional<Message> interpret(String message){

		// Ignore if empty
        if(message == null || message.trim().isEmpty()) return Optional.empty();

		// Split string
		String[] tokens = message.trim().split(":");

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
                default:
                    return Optional.empty(); // Invalid message
			}

			// Return the message
            return Optional.of(new Message(type, null, null, tokens[1].trim(), null));

		}else if(tokens.length == 3){

			// Check if correct tag
            if(!tokens[0].trim().equals("u")) return Optional.empty();

			// Return message
            return Optional.of(new Message(Type.UNLOCK, null, null, tokens[2].trim(), null));

		}else if(tokens.length == 5){

			// Check if correct tag
            if(!tokens[0].trim().equals("l")) return Optional.empty();

			try{
                return Optional.of(new Message(
                        Type.LOCK,
                        tokens[1].trim(),
                        tokens[2].trim(),
                        tokens[4].trim(),
                        Long.parseLong(tokens[3])
                ));
			}catch(NumberFormatException ignored){
                return Optional.empty();
			}
		}

		// If arrived here, then the message is invalid
        return Optional.empty();
    }

    @Nonnull
    public Message.Type getType(){
        return this.type;
    }

    @Nonnull
    public Optional<String> getClientId(){
        return Optional.ofNullable(this.client_id);
    }

    @Nonnull
    public String getLockpoint(){
        return this.lockpoint;
    }

    @Nonnull
    public Optional<String> getLockId(){
        return Optional.ofNullable(this.lock_id);
    }

    @Nonnull
    public Optional<Long> getLeaseTime(){
        return Optional.ofNullable(this.lease_time);
    }

    @Override
    @Nonnull
    public String toString(){
        return "Message(type=" + type + ", client_id=" + client_id + ", lock_id=" + lock_id + ", lockpoint=" + lockpoint + ", lease_time=" + lease_time + ")";
    }

    /** enum **/
    public enum Type{
        LOCK,
        UNLOCK,
        SHARED,
        OPEN,
        FREE
    }
}
