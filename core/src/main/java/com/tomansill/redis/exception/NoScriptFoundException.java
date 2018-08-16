package com.tomansill.redis.exception;

/** NoScriptFoundException class
 *  This is class used for throwing exception when script is not loaded on the database
 *  @author <a href="mailto:tom@ansill.com">Tom Ansill</a>
 */
public class NoScriptFoundException extends Exception{

    /** Script hash */
    private final String script_hash;

    /** Constructs the exception
     *  @param script_hash hash used to attempt to load the non-existent script on the database
     */
    public NoScriptFoundException(final String script_hash){
        super("Script hash '" + script_hash +"' cannot be found on the server");
        this.script_hash = script_hash;
    }

    /** Retrieves the hash that was used to load the script
     *  @return script hash
     */
    public String getScriptHash(){
        return this.script_hash;
    }
}
