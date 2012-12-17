/*
 * Copyright (c) 2011. Elad Kehat.
 * This software is provided under the MIT License:
 * http://www.opensource.org/licenses/mit-license.php
 */

package com.jzboy.couchdb;

import com.jzboy.couchdb.util.JsonUtils;
import java.io.IOException;
import org.codehaus.jackson.JsonNode;

/**
 * A wrapper around a CouchDB document.
 * <p>
 * A document contains:<br>
 * <ul>
 * <li>id	- its unique id in the database</li>
 * <li>rev	- its revision number</li>
 * <li>key	- if this document was retrieved from a view, its key in the view</li>
 * <li>json	- any other document or content, as a JSON object</li>
 * </ul>
 */
public class Document {

	String id = null;
	String rev = null;
	String key = null;
	JsonNode json = null;

	public Document(JsonNode json) {
		this.json = json;
	}

	public Document(String id, JsonNode json) {
		this.id = id;
		this.json = json;
	}

	public Document(String id, String rev, JsonNode json) {
		this.id = id;
		this.rev = rev;
		this.json = json;
	}

	public Document(String id, String rev) {
		this.id = id;
		this.rev = rev;
	}

	public Document(String id, String rev, String key, JsonNode json) {
		this.id = id;
		this.rev = rev;
		this.key = key;
		this.json = json;
	}

	public Document(String id, String rev, String jsonStr) throws IOException {
		this.id = id;
		this.rev = rev;
		this.json = JsonUtils.createParser(jsonStr).readValueAsTree();
	}

	public String getId() {
		return id;
	}

	public boolean hasId() {
		return (this.id != null);
	}

	public void setId(String id) {
		this.id = id;
	}

	public JsonNode getJson() {
		return json;
	}

	public void setJson(JsonNode json) {
		this.json = json;
	}

	/**
	 * Check whether this Document has a non-null JSON object
	 */
	public boolean hasContent() {
		return (this.json != null);
	}

	public String getKey() {
		return key;
	}

	public boolean hasKey() {
		return (this.key != null);
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getRev() {
		return rev;
	}

	public boolean hasRev() {
		return (this.rev != null);
	}

	public void setRev(String rev) {
		this.rev = rev;
	}

}
