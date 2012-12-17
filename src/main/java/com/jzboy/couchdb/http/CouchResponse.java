/*
 * Copyright (c) 2011. Elad Kehat.
 * This software is provided under the MIT License:
 * http://www.opensource.org/licenses/mit-license.php
 */

package com.jzboy.couchdb.http;

import com.jzboy.couchdb.util.JsonUtils;
import java.io.IOException;
import org.codehaus.jackson.JsonNode;

/**
 * Encapsulates a response from CouchDB api, including the HTTP response code, the actual response text (if any), and
 * the JsonNode parsed from that text
 */
public class CouchResponse {

	int statusCode;
	String statusPhrase;
	String body;
	JsonNode bodyAsJson;

	public CouchResponse(int statusCode, String statusPhrase, String body) {
		this.statusCode = statusCode;
		this.statusPhrase = statusPhrase;
		this.body = body;
		this.bodyAsJson = null;
	}

	public String getBody() {
		return body;
	}

	public JsonNode getBodyAsJson() throws IOException {
		// If two threads get here concurrently, it's possible for both to parse the body.
		// However, this is an unlikely event, since it's unusual for multiple threads to be processing the same
		// response. Even if it happens, the parse result should be the same.
		// So the overhead of synchronizing this method, which is called frequently, outweighs the "risk".
		if (bodyAsJson == null)
			bodyAsJson = JsonUtils.createParser(body).readValueAsTree();
		return bodyAsJson;
	}

	public int getStatusCode() {
		return statusCode;
	}

	public String getStatusPhrase() {
		return statusPhrase;
	}

}
