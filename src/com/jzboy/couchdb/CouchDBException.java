package com.jzboy.couchdb;

/**
 * A checked exception for the CouchDB code.
 * Optionally stores the http status code received in the response form couch.
 *
 * @author Elad Kehat
 */
public class CouchDBException extends Exception {
	static final long serialVersionUID = -1796055831637850765L;
	
	private final int statusCode;

	public CouchDBException(int statusCode, String message) {
		super(message);
		this.statusCode = statusCode;
	}

	public CouchDBException(String message) {
		this(0, message);
	}

	public CouchDBException(int statusCode, String error, String reason) {
		this(statusCode, String.format("%s: %s", error, reason));
	}

	public CouchDBException(String error, String reason) {
		this(0, error, reason);
	}

	public int getStatusCode() {
        return this.statusCode;
    }

}
