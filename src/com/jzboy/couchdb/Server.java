/*
 * Copyright (c) 2011. Elad Kehat.
 * This software is provided under the MIT License:
 * http://www.opensource.org/licenses/mit-license.php
 */

package com.jzboy.couchdb;

import com.jzboy.couchdb.http.*;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import org.codehaus.jackson.JsonNode;

/**
 * A wrapper around CouchDB server level API
 *
 * @see <a href="http://wiki.apache.org/couchdb/API_Cheatsheet#CouchDB_Server_Level">CouchDB Wiki</a>
 */
public class Server {

	final CouchHttpClient httpclient = new CouchHttpClient();
	final String host;
	final int port;

	/**
	 * Create a Server with the given host name and port number
	 */
	public Server(String host, int port) {
		this.host = host;
		this.port = port;
	}

	/**
	 * Create a Server running at the default location - localhost on port 5984
	 */
	public Server() {
		this("127.0.0.1", 5984);
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	/**
	 * Get the CouchHttpClient maintained by this server.
	 * It is safe to re-use this client by other objects that access CouchDB.
	 */
	public CouchHttpClient getHttpClient() {
		return httpclient;
	}

	/**
	 * Get the version number.
	 * Useful for determining whether the server is accessible.
	 * <p>
	 * The underlying API call returns a welcome message as well, but it is discarded.
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI from this server's host name and port
	 * @throws CouchDBException		if CouchDB returns an error - response code >= 300. The response details are
	 * encapsulated in the exception.
	 */
	public String version() throws IOException, URISyntaxException, CouchDBException {
		JsonNode json = httpclient.get(URITemplates.version(this));
		return json.get("version").getValueAsText();
	}

	/**
	 * Returns a list of all databases on this server.
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI from this server's host name and port
	 * @throws CouchDBException		if CouchDB returns an error - response code >= 300. The response details are
	 * encapsulated in the exception.
	 */
	public ArrayList<String> allDbs() throws IOException, URISyntaxException, CouchDBException  {
		JsonNode json = httpclient.get(URITemplates.allDbs(this));
		ArrayList<String> results = new ArrayList<String>();
		for (JsonNode item : json)
			results.add(item.getValueAsText());
		return results;
	}

	/**
	 * Returns this server's configuration data as a json object.
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI from this server's host name and port
	 * @throws CouchDBException		if CouchDB returns an error - response code >= 300. The response details are
	 * encapsulated in the exception.
	 */
	public JsonNode config() throws IOException, URISyntaxException, CouchDBException {
		return httpclient.get(URITemplates.config(this));
	}

	/**
	 * Returns this server's satistics as a json object.
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI from this server's host name and port
	 * @throws CouchDBException		if CouchDB returns an error - response code >= 300. The response details are
	 * encapsulated in the exception.
	 */
	public JsonNode stats() throws IOException, URISyntaxException, CouchDBException  {
		return httpclient.get(URITemplates.stats(this));
	}

	/**
	 * Returns a number of UUIDs that can be used as id's for new documents
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI from this server's host name and port
	 * @throws CouchDBException		if CouchDB returns an error - response code >= 300. The response details are
	 * encapsulated in the exception.
	 */
	public ArrayList<String> nextUUIDs(int count) throws IOException, URISyntaxException, CouchDBException  {
		JsonNode json = httpclient.get(URITemplates.uuids(this, count));
		ArrayList<String> uuids = new ArrayList<String>();
		for (JsonNode item : json.get("uuids"))
			uuids.add(item.getTextValue());
		return uuids;
	}

	/**
	 * Returns a single UUID
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI from this server's host name and port
	 * @throws CouchDBException		if CouchDB returns an error - response code >= 300. The response details are
	 * encapsulated in the exception.
	 */
	public String nextUUID() throws IOException, URISyntaxException, CouchDBException {
		return nextUUIDs(1).get(0);
	}

	/**
	 * Returns a list of active tasks (as JSON objects) that are running on the server.
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI from this server's host name and port
	 * @throws CouchDBException		if CouchDB returns an error - response code >= 300. The response details are
	 * encapsulated in the exception.
	 */
	public ArrayList<JsonNode> activeTasks() throws IOException, URISyntaxException, CouchDBException  {
		JsonNode result = httpclient.get(URITemplates.activeTasks(this));
		ArrayList<JsonNode> tasks = new ArrayList<JsonNode>();
		for (JsonNode item : result)
			tasks.add(item);
		return tasks;
	}

	/*
	 * Trigger a replication between two databases.
	 * The method copies the strings in source and target as-is into the JSON posted to CouchDB.
	 * @param source		the source database
	 * @param target		the target databae
	 * @param continuous	if true, this will trigger a continuous replication
	 * @see <a href="http://wiki.apache.org/couchdb/Replication">CouchDB Wiki
	 * for more info on how to specify the source and destination</a>
	 */
	/* @todo this method needs a better implementation, that deals with long timeouts
	public JsonNode replicate(String source, String target, boolean continuous) {
		ObjectNode root = JsonNodeFactory.instance.objectNode();
		root.put("source", source);
		root.put("target", target);
		if (continuous)
			root.put("continuous", true);
		return httpclient.post(URITemplates.replicate(this), JsonUtils.serializeJson(root));
	}
	 */

	@Override
	public String toString() {
		String url = null;
		try {
			url = URITemplates.version(this).toString();
		} catch (java.net.URISyntaxException e) {
			url = String.format("http://%s:%d/", host, port);
		}
		return "CouchDB @" + url;
	}

}
