/*
 * Copyright (c) 2011. Elad Kehat.
 * This software is provided under the MIT License:
 * http://www.opensource.org/licenses/mit-license.php
 */

package com.jzboy.couchdb.http;

import com.jzboy.couchdb.Database;
import com.jzboy.couchdb.Document;
import com.jzboy.couchdb.Server;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.http.client.utils.URIUtils;

/**
 * Utility methods that creates the URIs for access to CouchDB API.
 *
 * @see <a href="http://wiki.apache.org/couchdb/URI_templates">CouchDB Wiki</a>
 */
public class URITemplates {

	/**
	 * URI used to get the server's version
	 */
	public static URI version(String host, int port) throws URISyntaxException {
		return URIUtils.createURI("http", host, port, null, null, null);
	}

	public static URI version(Server server) throws URISyntaxException {
		return version(server.getHost(), server.getPort());
	}

	/**
	 * URI used to see a listing of databases
	 */
	public static URI allDbs(String host, int port) throws URISyntaxException {
		return URIUtils.createURI("http", host, port, "_all_dbs", null, null);
	}

	public static URI allDbs(Server server) throws URISyntaxException {
		return allDbs(server.getHost(), server.getPort());
	}

	/**
	 * URI used to see a server's configuration
	 */
	public static URI config(String host, int port) throws URISyntaxException {
		return URIUtils.createURI("http", host, port, "_config", null, null);
	}

	public static URI config(Server server) throws URISyntaxException {
		return config(server.getHost(), server.getPort());
	}

	/**
	 * URI used to get UUIDs from the server
	 */
	public static URI uuids(String host, int port, int count) throws URISyntaxException {
		String query = (count > 0) ? String.format("count=%d", count) : null;
		return URIUtils.createURI("http", host, port, "/_uuids", query, null);
	}

	public static URI uuids(Server server, int count) throws URISyntaxException {
		return uuids(server.getHost(), server.getPort(), count);
	}

	/**
	 * URI used to see a server's statistics
	 */
	public static URI stats(String host, int port) throws URISyntaxException {
		return URIUtils.createURI("http", host, port, "_stats", null, null);
	}

	public static URI stats(Server server) throws URISyntaxException {
		return stats(server.getHost(), server.getPort());
	}

	/**
	 * URI used to see active tasks running on a server
	 */
	public static URI activeTasks(String host, int port) throws URISyntaxException {
		return URIUtils.createURI("http", host, port, "_active_tasks", null, null);
	}

	public static URI activeTasks(Server server) throws URISyntaxException {
		return activeTasks(server.getHost(), server.getPort());
	}

	/**
	 * URI used to start a replication process between two databases
	 */
	public static URI replicate(String host, int port) throws URISyntaxException {
		return URIUtils.createURI("http", host, port, "_replicate", null, null);
	}

	public static URI replicate(Server server) throws URISyntaxException {
		return replicate(server.getHost(), server.getPort());
	}

	/**
	 * URI used to identify a database
	 */
	public static URI database(String host, int port, String dbName) throws URISyntaxException {
		return URIUtils.createURI("http", host, port, dbName, null, null);
	}

	public static URI database(Database db) throws URISyntaxException {
		return database(db.getServer().getHost(), db.getServer().getPort(), db.getDbName());
	}

	/**
	 * URI used to see a listing of multiple documents, filtered by the query parameters
	 */
	public static URI allDocs(String host, int port, String dbName, String query) throws URISyntaxException {
		return URIUtils.createURI("http", host, port, dbName + "/_all_docs", query, null);
	}

	/**
	 * URI used to see a listing of all the data documents in a database
	 */
	public static URI allDocs(String host, int port, String dbName) throws URISyntaxException {
		return allDocs(host, port, dbName, null);
	}

	public static URI allDocs(Database db) throws URISyntaxException {
		return allDocs(db.getServer().getHost(), db.getServer().getPort(), db.getDbName());
	}

	public static URI allDocs(Database db, String query) throws URISyntaxException {
		return allDocs(db.getServer().getHost(), db.getServer().getPort(), db.getDbName(), query);
	}

	/**
	 * URI used in bulk document operations
	 */
	public static URI bulkDocs(String host, int port, String dbName) throws URISyntaxException {
		return URIUtils.createURI("http", host, port, dbName + "/_bulk_docs", null, null);
	}

	public static URI bulkDocs(Database db) throws URISyntaxException {
		return bulkDocs(db.getServer().getHost(), db.getServer().getPort(), db.getDbName());
	}

	/**
	 * URI used to see a listing of changes made to documents in a database
	 * @see <a href="http://wiki.apache.org/couchdb/HTTP_database_API#Changes">CouchDB Wiki</a>
	 */
	public static URI dbChanges(String host, int port, String dbName, String params) throws URISyntaxException {
		return URIUtils.createURI("http", host, port, dbName + "/_changes", params, null);
	}

	public static URI dbChanges(Database db, String params) throws URISyntaxException {
		return dbChanges(db.getServer().getHost(), db.getServer().getPort(), db.getDbName(), params);
	}

	/**
	 * URI used to get/set an upper bound on the number document revisions that CouchDB keeps track of
	 * @see <a href="http://wiki.apache.org/couchdb/HTTP_database_API#Accessing_Database-specific_options">CouchDB Wiki</a>
	 */
	public static URI revsLimit(String host, int port, String dbName) throws URISyntaxException {
		return URIUtils.createURI("http", host, port, dbName + "/_revs_limit", null, null);
	}

	public static URI revsLimit(Database db) throws URISyntaxException {
		return revsLimit(db.getServer().getHost(), db.getServer().getPort(), db.getDbName());
	}

	/**
	 * URI used to trigger database compaction
	 * @see <a href="http://wiki.apache.org/couchdb/Compaction">CouchDB Wiki</a>
	 */
	public static URI compact(String host, int port, String dbName) throws URISyntaxException {
		return URIUtils.createURI("http", host, port, dbName + "/_compact", null, null);
	}

	public static URI compact(Database db) throws URISyntaxException {
		return compact(db.getServer().getHost(), db.getServer().getPort(), db.getDbName());
	}

	/**
	 * URI used to trigger view compaction
	 * @see <a href="http://wiki.apache.org/couchdb/Compaction#View_compaction">CouchDB Wiki</a>
	 */
	public static URI compactView(String host, int port, String dbName, String designDocName) throws URISyntaxException	{
		String path = String.format("%s/_compact/%s", dbName, designDocName);
		return URIUtils.createURI("http", host, port, path, null, null);
	}

	public static URI compactView(Database db, String designDocName) throws URISyntaxException {
		return compactView(db.getServer().getHost(), db.getServer().getPort(), db.getDbName(), designDocName);
	}

	/**
	 * URI used to clean up outdated view indexes
	 * @see <a href="http://wiki.apache.org/couchdb/Compaction#View_compaction">CouchDB Wiki</a>
	 */
	public static URI cleanupViews(String host, int port, String dbName) throws URISyntaxException {
		return URIUtils.createURI("http", host, port, dbName + "/_view_cleanup", null, null);
	}

	public static URI cleanupViews(Database db) throws URISyntaxException {
		return cleanupViews(db.getServer().getHost(), db.getServer().getPort(), db.getDbName());
	}

	/**
	 * URI used to manipulate a document
	 */
	public static URI document(String host, int port, String dbName, String docId, boolean batch) throws URISyntaxException {
		String path = String.format("%s/%s", dbName, docId);
		String query = batch ? "batch=ok" : null;
		return URIUtils.createURI("http", host, port, path, query, null);
	}

	public static URI document(Database db, String docId, boolean batch) throws URISyntaxException {
		return document(db.getServer().getHost(), db.getServer().getPort(), db.getDbName(), docId, batch);
	}

	public static URI document(Database db, String docId) throws URISyntaxException {
		return document(db.getServer().getHost(), db.getServer().getPort(), db.getDbName(), docId, false);
	}

	/**
	 * URI used to post a new document, with server-generated docId
	 */
	public static URI document(Database db, boolean batch) throws URISyntaxException {
		return document(db.getServer().getHost(), db.getServer().getPort(), db.getDbName(), null, batch);
	}

	/**
	 * URI used to delete a document
	 */
	public static URI deleteDocument(Database db, Document doc) throws URISyntaxException {
		String path = String.format("%s/%s",  db.getDbName(), doc.getId());
		String query = "rev=" + doc.getRev();
		return URIUtils.createURI("http", db.getServer().getHost(), db.getServer().getPort(), path, query, null);
	}

	/**
	 * URI used to download a file attachment
	 */
	public static URI attachment(String host, int port, String dbName, String docId, String fileName)
			throws URISyntaxException
	{
		String path = String.format("%s/%s/%s", dbName, docId, fileName);
		return URIUtils.createURI("http", host, port, path, null, null);
	}

	public static URI attachment(Database db, String docId, String fileName) throws URISyntaxException {
		return attachment(db.getServer().getHost(),  db.getServer().getPort(), db.getDbName(), docId, fileName);
	}

	/**
	 * URI used for CRUD operations on an attachment
	 */
	public static URI attachment(Database db, Document doc, String fileName) throws URISyntaxException {
		String query = doc.hasRev() ? "rev=" + doc.getRev() : null;
		String path = String.format("%s/%s/%s",  db.getDbName(), doc.getId(), fileName);
		return URIUtils.createURI("http", db.getServer().getHost(), db.getServer().getPort(), path, query, null);
	}

	/**
	 * URI used to see a design document
	 */
	public static URI designDoc(String host, int port, String dbName, String designDocName)
			throws URISyntaxException
	{
		String path = String.format("%s/_design/%s", dbName, designDocName);
		return URIUtils.createURI("http", host, port, path, null, null);
	}

	public static URI designDoc(Database db, String designDocName) throws URISyntaxException {
		return designDoc(db.getServer().getHost(), db.getServer().getPort(), db.getDbName(), designDocName);
	}

	/**
	 * URI used to get information about a design document
	 */
	public static URI designDocInfo(String host, int port, String dbName, String designDocName)
			throws URISyntaxException
	{
		String path = String.format("%s/_design/%s/_info", dbName, designDocName);
		return URIUtils.createURI("http", host, port, path, null, null);
	}

	public static URI designDocInfo(Database db, String designDocName) throws URISyntaxException {
		return designDocInfo(db.getServer().getHost(), db.getServer().getPort(), db.getDbName(), designDocName);
	}

	/**
	 * URI used to query a view
	 */
	public static URI queryView(String host,
										int port,
										String dbName,
										String designDocName,
										String viewName,
										String query) throws URISyntaxException
	{
		String path = String.format("%s/_design/%s/_view/%s", dbName, designDocName, viewName);
		return URIUtils.createURI("http", host, port, path, query, null);	
	}

	public static URI queryView(Database db, String designDocName, String viewName,	String query)
			throws URISyntaxException
	{
		return queryView(db.getServer().getHost(),
						 db.getServer().getPort(),
						 db.getDbName(),
						 designDocName,
						 viewName,
						 query);
	}

	/**
	 * URI used to run a temporary (ad-hoc) view
	 */
	public static URI tempView(String host, int port, String dbName) throws URISyntaxException {
		return URIUtils.createURI("http", host, port, dbName + "/_temp_view", null, null);
	}

	public static URI tempView(Database db) throws URISyntaxException {
		return tempView(db.getServer().getHost(), db.getServer().getPort(), db.getDbName());
	}

	/**
	 * URI used to format a document through a "show" template
	 */
	public static URI formatDoc(String host,
										int port,
										String dbName,
										String designDocName,
										String showName,
										String docId)
			throws URISyntaxException
	{
		String path = String.format("%s/_design/%s/_show/%s/%s", dbName, designDocName, showName, docId);
		return URIUtils.createURI("http", host, port, path, null, null);
	}

	/**
	 * URI used to format a view through a "list" template
	 */
	public static URI formatView(String host,
										int port,
										String dbName,
										String designDocName,
										String listName,
										String viewName,
										String query)
			throws URISyntaxException
	{
		String path = String.format("%s/_design/%s/_list/%s/%s", dbName, designDocName, listName, viewName);
		return URIUtils.createURI("http", host, port, path, query, null);
	}

}
