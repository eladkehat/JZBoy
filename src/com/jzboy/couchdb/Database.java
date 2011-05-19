/*
 * Copyright (c) 2011. Elad Kehat.
 * This software is provided under the MIT License:
 * http://www.opensource.org/licenses/mit-license.php
 */

package com.jzboy.couchdb;

import com.jzboy.couchdb.http.CouchResponse;
import com.jzboy.couchdb.util.JsonUtils;
import com.jzboy.couchdb.http.URITemplates;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper around CouchDB database level API
 *
 * @see <a href="http://wiki.apache.org/couchdb/API_Cheatsheet">CouchDB API Wiki</a>
 */
public class Database {

	private static final Logger logger = LoggerFactory.getLogger(Database.class);

	final Server server;
	final String dbName;

	private ArrayList<Document> bulkUpdatesCache = new ArrayList<Document>();
	private int bulkUpdatesLimit = 1000;

	/**
	 * Create a Database object
	 * @param server	a Server that contains information on the CouchDB server
	 * @param dbName	unique name of this database on the server
	 */
	public Database(Server server, String dbName) {
		this.server = server;
		this.dbName = dbName;
	}

	/**
	 * Create a Database object, and its encapsulated Server
	 * @param host		the server's host
	 * @param port		the server's port
	 * @param dbName	unique name of this database on the server
	 */
	public Database(String host, int port, String dbName) {
		this.server = new Server(host, port);
		this.dbName = dbName;
	}

	/**
	 * Create a Database object, and a default encapsulated Server.
	 * The encapsulated server points to localhost:5984
	 * @param dbName	unique name of this database on the server
	 */
	public Database(String dbName) {
		this.server = new Server();
		this.dbName = dbName;
	}

	public String getDbName() {
		return dbName;
	}

	public Server getServer() {
		return server;
	}

	/**
	 * Returns the current size of the bulk updates cache.
	 * When using {@link #saveInBulk(com.jzboy.couchdb.Document) saveInBulk}, this size is used to determine
	 * after how many documents the whole cache is saved in a bulk operation.
	 */
	public int getBulkUpdatesLimit() {
		return bulkUpdatesLimit;
	}

	/**
	 * Set the size of the bulk updates cache.
	 * This <strong>will not</strong> automatically flush the cache if its current size is greater than the new limit.
	 */
	public void setBulkUpdatesLimit(int bulkUpdatesLimit) {
		if (bulkUpdatesLimit < 1)
			throw new IllegalArgumentException("bulkUpdatesLimit must be greater than 0");
		this.bulkUpdatesLimit = bulkUpdatesLimit;
	}

	/**
	 * Returns information about the database.
	 * @see <a href="http://wiki.apache.org/couchdb/HTTP_database_API#Database_Information">
	 * CouchDB Wiki for details on the information returned</a>
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI for this database
	 * @throws CouchDBException		if CouchDB returns an error - response code >= 300. The response
	 * details are encapsulated in the exception.
	 */
	public JsonNode info() throws IOException, URISyntaxException, CouchDBException {
		return server.getHttpClient().get(URITemplates.database(this));
	}

	/**
	 * Returns information about the database.
	 * Parses the raw JSON result into a Java Map.
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI for this database
	 * @throws CouchDBException		if CouchDB returns an error - response code >= 300. The response
	 * details are encapsulated in the exception.
	 */
	public Map<String, String> infoAsMap() throws IOException, URISyntaxException, CouchDBException {
		HashMap<String, String> map = new HashMap<String, String>();
		Iterator<Map.Entry<String, JsonNode>> it = ((ObjectNode) info()).getFields();
		while (it.hasNext()) {
			Map.Entry<String, JsonNode> entry = it.next();
			map.put(entry.getKey(), entry.getValue().getValueAsText());
		}
		return map;
	}

	/**
	 * Check if this Database exists on the server.
	 * This is a better way than calling {@link #info} and catching an exception, as it avoids the
	 * exception's overhead. If CouchDB's response is not 200 (exists) or 404 (missing), then
	 * the usual CocuhDBException is thrown.
	 * @return	true iff a database with this name exists on the server
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI for this database
	 * @throws CouchDBException		if CouchDB returns an error - response code other than 200 or 404.
	 */
	public boolean exists() throws IOException, URISyntaxException, CouchDBException {
		CouchResponse res = server.getHttpClient().getCouchResponse(URITemplates.database(this));
		if (res.getStatusCode() == 200)
			return true;
		else if (res.getStatusCode() == 404)
			return false;
		else
			throw new CouchDBException(res.getStatusCode(), res.getBody());
	}

	/**
	 * Creates this database
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI for this database
	 * @throws CouchDBException		if CouchDB returns an error - response code >= 300. The response
	 * details are encapsulated in the exception.	 */
	public void create() throws IOException, URISyntaxException, CouchDBException {
		server.getHttpClient().put(URITemplates.database(this));
	}

	/**
	 * Tests if this database exists. If it doesn't, create it.
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI for this database
	 * @throws CouchDBException		if CouchDB returns an error.
	 */
	public void createIfNotExists() throws IOException, URISyntaxException, CouchDBException {
		if (!exists())
			create();
	}

	/**
	 * Delete this database
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI for this database
	 * @throws CouchDBException		if CouchDB returns an error.
	 */
	public void delete() throws IOException, URISyntaxException, CouchDBException {
		// On Windows, an attempt to delete the database frequently results in a eaccess(500) error.
		// see - https://issues.apache.org/jira/browse/COUCHDB-326
		// The code here retries the request several times on such errors
		int attemptsLeft = 5;
		boolean retry = true;
		while (retry && attemptsLeft > 0) {
			try {
				server.getHttpClient().delete(URITemplates.database(this));
				retry = false;
			} catch (CouchDBException ex) {
				if (ex.getStatusCode() == 500 && attemptsLeft > 1) {
					attemptsLeft--;
					logger.debug("Got {} on delete attempt #{} on {}",
								 new Object[] {ex.getMessage(), (3-attemptsLeft), this});
					// wait for a short while between delete attempts
					try { Thread.sleep(100); } catch (InterruptedException ie) { }
				} else {
					throw ex;
				}
			}
		}
	}

	/**
	 * Returns the upper bound of document revisions which CouchDB keeps track of.
	 * @see <a href="http://wiki.apache.org/couchdb/HTTP_database_API#Accessing_Database-specific_options">
	 * CouchDB Wiki for more</a>
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI for this database
	 * @throws CouchDBException		if CouchDB returns an error - response code >= 300. The response
	 * details are encapsulated in the exception.
	 */
	public int getRevsLimit() throws IOException, URISyntaxException, CouchDBException {
		String res = server.getHttpClient().getBody(URITemplates.revsLimit(this));
		// the result is followed by a newline, which must be trimmed or parseInt fails
		return Integer.parseInt(res.trim());
	}

	/**
	 * Set the upper bound of document revisions which CouchDB keeps track of.
	 * @see <a href="http://wiki.apache.org/couchdb/HTTP_database_API#Accessing_Database-specific_options">
	 * CouchDB Wiki</a>
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI for this database
	 * @throws CouchDBException		if CouchDB returns an error - response code >= 300. The response
	 * details are encapsulated in the exception.
	 */
	public void setRevsLimit(int limit) throws IOException, URISyntaxException, CouchDBException {
		server.getHttpClient().put(URITemplates.revsLimit(this), String.valueOf(limit));
	}

	/**
	 * Returns a list of changes made to documents in the database.
	 * This method doesn't support the 'continuous' option and throws an exception if it is
	 * present in the params map.
	 * @param params	parameters for the Changes API call. Keys are parameter names, mapped
	 * to the string representation of the value, which is copied as-is to the JSON sent to
	 * CouchDB. It is OK to pass either null or an empty map.
	 * @return	the list of changes, where each change is a JSON
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI for this database
	 * @throws CouchDBException		if CouchDB returns an error - response code >= 300. The response
	 * details are encapsulated in the exception.
	 * @see <a href="http://wiki.apache.org/couchdb/HTTP_database_API#Changes">
	 * CouchDB Wiki for a description of the possible parameters</a>
	 */
	public ArrayList<JsonNode> changes(List<NameValuePair> params) 
			throws IOException, URISyntaxException, CouchDBException
	{
		if (params.contains(new BasicNameValuePair("continuous", "true")))
			throw new CouchDBException("'continuous' feeds are not supported by this method");

		JsonNode res = server.getHttpClient().get(
				URITemplates.dbChanges(this, URLEncodedUtils.format(params, "utf-8")));
		ArrayList<JsonNode> results = new ArrayList<JsonNode>();
		for (JsonNode row : res.get("results"))
			results.add(row);
		return results;
	}

	/**
	 * Triggers database compaction.
	 * @see <a href="http://wiki.apache.org/couchdb/Compaction#Database_Compaction">CouchDB Wiki</a>
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI for this database
	 * @throws CouchDBException		if CouchDB returns an error - response code >= 300. The response
	 * details are encapsulated in the exception.
	 */
	public void compact() throws IOException, URISyntaxException, CouchDBException {
		server.getHttpClient().post(URITemplates.compact(this));
	}

	/**
	 * Retrieve a document.
	 * @param id	identifier of the document to retrieve
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI for this database
	 * @throws CouchDBException		if the document is missing, throws a CouchDBException with 404
	 * as its status code
	 */
	public Document getDocument(String id) throws IOException, URISyntaxException, CouchDBException {
		JsonNode json = server.getHttpClient().get(URITemplates.document(this, id));
		return buildDocumentFromJsonResponse(json);
	}

	/**
	 * Retrieve a document. If that document is missing, returns null rather than throw an exception.
	 * Use this to check if a document exists without the overhead of exception handling.
	 * Note that if CouchDB's responds with an error code other than or 404, then the usual
	 * CocuhDBException is thrown.
	 * @param id	identifier of the document to retrieve
	 * @return the document, or null if no document with this id is found
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI for this database
	 * @throws CouchDBException		on all CouchDB errors other than 404
	 */
	public Document getDocumentOrNull(String id) throws IOException, URISyntaxException, CouchDBException {
		CouchResponse res = server.getHttpClient().getCouchResponse(URITemplates.document(this, id));
		if (res.getStatusCode() >= 300) {
			if (res.getStatusCode() == 404)
				return null;
			else
				throw new CouchDBException(res.getStatusCode(), res.getBody());
		}
		return buildDocumentFromJsonResponse(res.getBodyAsJson());
	}

	private Document buildDocumentFromJsonResponse(JsonNode json) {
		ObjectNode obj = (ObjectNode) json;
		String rev = obj.remove("_rev").getTextValue();
		String retId = obj.remove("_id").getTextValue();
		return new Document(retId, rev, obj);
	}

	/**
	 * Parse a JSON response with a list of documents.
	 * The source JSON is the response from either an _all_docs or a view query.
	 * If the JSON rows contain a "doc" element (include_docs=true was passed as a parameter to the query), it is used
	 * in constructing the returned documents, and the contents of "value" elements in the rows are ignored. Otherwise,
	 * the returned documents are constructed with the arbitrary JSON inside the "value" element.
	 */
	private ArrayList<Document> parseDocuments(JsonNode json) {
		int totalRows = json.get("total_rows").getIntValue();
		ArrayList<Document> results = new ArrayList<Document>(totalRows);
		for (JsonNode row : json.get("rows")) {
			// if include_docs=true was in the params, we get the full document within each result row
			JsonNode docNode = row.get("doc");
			String id = row.get("id").getTextValue();
			String key = row.get("key").getTextValue();
			if (docNode != null) {
				ObjectNode doc = (ObjectNode) row.get("doc");
				String rev = doc.remove("_rev").getTextValue();
				doc.remove("_id").getTextValue(); // same as "id" retrieved from the row
				results.add(new Document(id, rev, key, doc));
			}
			else if (row.get("error") != null) {
				continue;
			} else { // docNode is null
				JsonNode value = row.get("value");
				JsonNode rev = null;
				if (value.isObject()) {
					ObjectNode objValue = (ObjectNode) value;
					rev = objValue.remove("rev");
					if (rev == null)
						rev = objValue.remove("_rev");
				}
				String revStr = (rev != null) ? rev.getTextValue() : null;
				Document doc = new Document(id, revStr, key, value);
				results.add(doc);
			}
		}
		return results;
	}

	/**
	 * Retrieve a collection of documents, given their ids.
	 * @param ids		ids of the documents
	 * @param params	view API params
	 * @return if params contain include_docs:true, a list of the full documents; otherwise each Document in the result
	 * list contains an id and rev only. Ids that weren't found are ignored, so the result's size may be smaller than
	 * the input ids'.
	 * @see <a href="http://wiki.apache.org/couchdb/HTTP_Bulk_Document_API#Fetch_Multiple_Documents_With_a_Single_Request">
	 * CouchDB Wiki for more information on valid parameters</a>
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI for this database
	 * @throws CouchDBException		if CouchDB returns an error - response code >= 300. The response
	 * details are encapsulated in the exception.
	 */
	public ArrayList<Document> getDocuments(Collection<String> ids, List<NameValuePair> params) 
			throws IOException, URISyntaxException, CouchDBException
	{
		// build a json document with the requested document ids
		ObjectNode root = JsonNodeFactory.instance.objectNode();
		ArrayNode keys = root.putArray("keys");
		for (String id : ids)
			keys.add(id);
		String data = JsonUtils.serializeJson(root);
		String query = (params == null) ? null : URLEncodedUtils.format(params, "utf-8");
		JsonNode json = server.getHttpClient().post(URITemplates.allDocs(this, query), data);
		return parseDocuments(json);
	}

	/**
	 * Convenience method that calls {@link #getDocuments(java.util.Collection, java.util.List) getDocuments}
	 * method with the include_docs=true param.
	 */
	public ArrayList<Document> getDocuments(Collection<String> ids, boolean includeDocs)
			throws IOException, URISyntaxException, CouchDBException
	{
		return getDocuments(ids, includeDocsParams(includeDocs));
	}

	/**
	 * Optionally return a 1-item list, with the name-value pair include_docs=true
	 * @return	a list with the include_docs param, or null if includeDocs is false
	 */
	private List<NameValuePair> includeDocsParams(boolean includeDocs) {
		List<NameValuePair> params = null;
		if (includeDocs)
			params = new ArrayList<NameValuePair>() {{ add(new BasicNameValuePair("include_docs", "true")); }};
		return params;
	}

	/**
	 * Retrieve a list of documents, optionally filtered by the parameters.
	 * @param params	view API params
	 * @return if params contain include_docs:true, a list of the full documents; otherwise each Document in the result
	 * list contains an id and rev only.
	 * @see <a href="http://wiki.apache.org/couchdb/HTTP_Bulk_Document_API#Fetch_Multiple_Documents_With_a_Single_Request">
	 * CouchDB Wiki for more information on valid parameters</a>
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI for this database
	 * @throws CouchDBException		if CouchDB returns an error - response code >= 300. The response
	 * details are encapsulated in the exception.
	 */
	public ArrayList<Document> getAllDocuments(List<NameValuePair> params) 
			throws IOException, URISyntaxException, CouchDBException
	{
		String query = (params == null) ? null : URLEncodedUtils.format(params, "utf-8");
		JsonNode json = server.getHttpClient().get(URITemplates.allDocs(this, query));
		return parseDocuments(json);
	}

	/**
	 * Convenience method that calls {@link #getAllDocuments(java.util.List) getAllDocuments} method with just the
	 * include_docs param.
	 */
	public ArrayList<Document> getAllDocuments(boolean includeDocs)
			throws IOException, URISyntaxException, CouchDBException
	{
		return getAllDocuments(includeDocsParams(includeDocs));
	}

	/**
	 * Create a document object from a result that contains "id" and "rev" fields.
	 * These results are usually returned by create/update/delete operations on documents.
	 */
	private Document parseDocumentResult(JsonNode json) {
		String retId = json.get("id").getTextValue();
		JsonNode revNode = json.get("rev");
		// no rev is returned when saving in batch mode
		String rev = (revNode == null) ? null : revNode.getTextValue();
		return new Document(retId, rev, json);
	}

	/**
	 * Create a new document with the specified id.
	 * @param uuid		the uuid given to the new docuemnt
	 * @param docJson	the document's content in JSON format
	 * @param batch		if true, instructs CouchDB to insert the document in batch mode and not
	 * right away
	 * @return a new Document, with the new revision number, and CouchDB's JSON response. This Document
	 * does not contain the original document content.
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI for this database
	 * @throws CouchDBException		if CouchDB returns an error - response code >= 300. The response
	 * details are encapsulated in the exception.
	 */
	public Document createDocument(String uuid, String docJson, boolean batch)
			throws IOException, URISyntaxException, CouchDBException
	{
		String doc = JsonUtils.reencodeUtf8ToIso88591(docJson);
		JsonNode json = server.getHttpClient().put(URITemplates.document(this, uuid, batch), doc);
		return parseDocumentResult(json);
	}

	public Document createDocument(String uuid, JsonNode json, boolean batch)
			throws IOException, URISyntaxException, CouchDBException
	{
		return createDocument(uuid, JsonUtils.serializeJson(json), batch);
	}

	/**
	 * Create a new document, with server-generated id.
	 */
	public Document createDocument(String docJson, boolean batch)
			throws IOException, URISyntaxException, CouchDBException
	{
		String doc = JsonUtils.reencodeUtf8ToIso88591(docJson);
		// use post, rather than put to have the server generate an id for us
		JsonNode json = server.getHttpClient().post(URITemplates.document(this, batch), doc);
		return parseDocumentResult(json);
	}

	public Document createDocument(JsonNode json, boolean batch)
			throws IOException, URISyntaxException, CouchDBException
	{
		return createDocument(JsonUtils.serializeJson(json), batch);
	}

	/**
	 * Creates a new document from the given Document object.
	 * If newDoc has no id field, then one is generated by the server.
	 */
	public Document createDocument(Document newDoc, boolean batch)
			throws IOException, URISyntaxException, CouchDBException
	{
		if (newDoc.hasId())
			return createDocument(newDoc.getId(), newDoc.getJson(), batch);
		else
			return createDocument(newDoc.getJson(), batch);
	}

	public Document createDocument(Document newDoc)
			throws IOException, URISyntaxException, CouchDBException
	{
		return createDocument(newDoc, false);
	}

	/**
	 * Given an existing document, update it.
	 * @param doc	the Document to update. Must contain the id and revision number. Must also contain
	 * the entire document data as a JSON - not just the new parts.
	 * @return a new Document, with the new revision number, and CouchDB's JSON response. This Document
	 * does not contain the original or updated document content.
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI for this database
	 * @throws CouchDBException		a common problem here is a document update conflict. In this case a
	 * CouchDBException is thrown, with 409 as its status code.
	 */
	public Document updateDocument(Document doc) throws IOException, URISyntaxException, CouchDBException
	{
		// CouchDB semantics do not allow us to update a doc that has no revision field, so avoid this in advance
		if (doc.getRev() == null)
			throw new CouchDBException(
					String.format("Cannot update %s - missing a current revision number", doc.getId()));

		ObjectNode root = (ObjectNode) doc.getJson();
		// add the current revision into the json string - as required by CouchDB for updates
		// if this isn't the latest revision we'll get a 409 response
		root.put("_rev", doc.getRev());
		String docValue = JsonUtils.reencodeUtf8ToIso88591(JsonUtils.serializeJson(root));
		// Connection resets are quite common with updates, so retry the update several times
		JsonNode json = server.getHttpClient().put(URITemplates.document(this, doc.getId()), docValue, 3);
		return parseDocumentResult(json);
	}

	/**
	 * Delete the document.
	 * @param doc	the Document to delete. Must include an id and revision. Its JSON content is ignored.
	 * @return the revision id for the deletion stub
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI for this database
	 * @throws CouchDBException	if the revision number is not up-to-date.
	 */
	public String deleteDocument(Document doc) throws IOException, URISyntaxException, CouchDBException {
		JsonNode json = server.getHttpClient().delete(URITemplates.deleteDocument(this, doc));
		return json.get("rev").getTextValue();
	}

	/**
	 * Get the speficied document attachment as a byte array.
	 * @param docId		id of the document to which the attachment belongs
	 * @param fileName	name of the attachment
	 * @return the streaming attachment
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI for this database
	 * @throws CouchDBException		if CouchDB returns an error - response code >= 300. The response
	 * details are encapsulated in the exception.
	 */
	public byte[] getAttachment(String docId, String fileName) 
			throws URISyntaxException, IOException, CouchDBException
	{
		HttpResponse res = server.getHttpClient().getHttpResponse(URITemplates.attachment(this, docId, fileName));
		if (res.getStatusLine().getStatusCode() >= 300) {
			res.getEntity().consumeContent(); // necessary in order to release the underlying connection
			throw new CouchDBException(res.getStatusLine().getStatusCode(), res.getStatusLine().getReasonPhrase());
		}
		return EntityUtils.toByteArray(res.getEntity());
	}

	/**
	 * Get the speficied document attachment, as a raw HttpResponse.
	 * Use this method get get low-level access, for example, to the attachment's content-type header, or
	 * to read an attachment from a stream.
	 * Make sure you release the underlying resources - @see
	 * <a href="http://hc.apache.org/httpcomponents-client/tutorial/html/fundamentals.html#d4e143">
	 * HttpComponents Tutorial</a>
	 * @param docId		id of the document to which the attachment belongs
	 * @param fileName	name of the attachment
	 * @return the HttpRespone object returned by executing a get request for the attachment
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI for this database
	 */
	public HttpResponse getAttachmentRaw(String docId, String fileName) throws IOException, URISyntaxException {
		return server.getHttpClient().getHttpResponse(URITemplates.attachment(this, docId, fileName));
	}

	/**
	 * Create or update a document attachment.
	 * @param doc		the document to which the attachment belongs. If the document object contains no revision id,
	 * a new document is created along with the attachment.
	 * @param fileName	name of the attachment. If it matches an attachment that already exists for this document,
	 * then it is updated. Otherwise, a new attachment is created for the document.
	 * @param data		the attachment itself, as a byte array
	 * @param mimeType	MIME type of the attachment
	 * @return a new Document, with the new revision number, and CouchDB's JSON response. This Document
	 * does not contain the original or updated document content.
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI for this database
	 * @throws CouchDBException		if CouchDB returns an error - response code >= 300. The response
	 * details are encapsulated in the exception.
	 */
	public Document saveAttachment(Document doc, String fileName, byte[] data, String mimeType)
			throws IOException, URISyntaxException, CouchDBException
	{
		JsonNode json =	server.getHttpClient().put(URITemplates.attachment(this, doc, fileName), data, mimeType);
		return parseDocumentResult(json);
	}

	/**
	 * Delete a document attachment.
	 * @param doc		the document ot which the attachment belongs. Must contain both an id and a revision.
	 * @param fileName	name of the attachment to delete
	 * @return			a document object with the updated revision number
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI for this database
	 * @throws CouchDBException		if CouchDB returns an error - response code >= 300. The response
	 * details are encapsulated in the exception.
	 */
	public Document deleteAttachment(Document doc, String fileName)
			throws IOException, URISyntaxException, CouchDBException
	{
		JsonNode json =	server.getHttpClient().delete(URITemplates.attachment(this, doc, fileName));
		return parseDocumentResult(json);
	}

	/**
	 * Save the document in bulk mode: add the new document to an internal buffer rather than sending it to
	 * CouchDB right away.
	 * <p>
	 * Bulk inserts increase performance by a factor of 500! (See O'Reilly - CouchDB - The Definitive Guide)
	 * <p>
	 * The bulk cache is flushed when the number of documents in it reaches bulkCacheLimit, or when flushBulkCache
	 * is called.
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI for this database
	 * @throws CouchDBException		if CouchDB returns an error - response code >= 300. The response
	 * details are encapsulated in the exception.
	 */
	public void saveInBulk(Document doc) throws IOException, URISyntaxException, CouchDBException {
		// if the document has no JSON content, then there's nothing to save
		if (!doc.hasContent())
			throw new IllegalArgumentException("Document has no JSON content");
		
		synchronized(this) {
			bulkUpdatesCache.add(doc);
			if (bulkUpdatesCache.size() >= bulkUpdatesLimit)
				flushBulkUpdatesCache();
		}
	}

	/**
	 * Delete the document in bulk mode - calls {@link #saveInBulk(com.jzboy.couchdb.Document) saveInBulk}, adding
	 * a _deleted=true element to the document.
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI for this database
	 * @throws CouchDBException		if CouchDB returns an error - response code >= 300. The response
	 * details are encapsulated in the exception.
	 */
	public void deleteInBulk(Document doc) throws IOException, URISyntaxException, CouchDBException {
		ObjectNode json;
		if (doc.getJson() == null) {
			json = JsonNodeFactory.instance.objectNode();
			doc.setJson(json);
		} else {
			json = ((ObjectNode) doc.getJson());
		}
		json.put("_deleted", true);
		saveInBulk(doc);
	}

	/**
	 * Saves a collection of document, stored internally in a cache, to CouchDB.
	 * @param allOrNothing	update in bulk using transactional semantics.
	 * @param returnReport	if true returns the results received from CouchDB. The JSON may be large and takes a
	 * while to process, so if don't plan on doing anything with it, pass false to avoid this processing and the
	 * method will return null.
	 * @see <a href="http://wiki.apache.org/couchdb/HTTP_Bulk_Document_API#Transactional_Semantics_with_Bulk_Updates">
	 * CouchDB Wiki on transactional semantics for more information regarding the allOrNothing option</a>
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI for this database
	 * @throws CouchDBException		if CouchDB returns an error - response code >= 300. The response
	 * details are encapsulated in the exception.
	 */
	public synchronized ArrayList<JsonNode> flushBulkUpdatesCache(boolean allOrNothing, boolean returnReport)
			throws IOException, URISyntaxException, CouchDBException
	{
		if (bulkUpdatesCache.isEmpty())
			return (returnReport) ? new ArrayList<JsonNode>() : null;
		// build a json document from all docs in the cache
		ObjectNode root = JsonNodeFactory.instance.objectNode();
		ArrayNode docs = root.putArray("docs");
		for (Document doc : bulkUpdatesCache) {
			ObjectNode node = (ObjectNode) doc.getJson();
			if (doc.hasId()) // if the doc has no id, a new one is assigned to it automatically by CouchDB
				node.put("_id", doc.getId());
			if (doc.hasRev()) // if the doc has a revision, it's updated rather than inserted
				node.put("_rev", doc.getRev());
			docs.add(node);
		}
		if (allOrNothing)
			root.put("all_or_nothing", true);

		String jsonStr = JsonUtils.reencodeUtf8ToIso88591(JsonUtils.serializeJson(root));
		// according to the docs (http://wiki.apache.org/couchdb/HTTP_Bulk_Document_API) we'll never get an error here
		// - problems are reported on an individual node basis
		JsonNode res = server.getHttpClient().post(URITemplates.bulkDocs(this), jsonStr);
		bulkUpdatesCache.clear();
		
		if (!returnReport)
			return null;
		ArrayList<JsonNode> results = new ArrayList<JsonNode>();
		for (JsonNode row : res)
			results.add(row);
		return results;
	}

	/**
	 * Convenience method that calls flushBulkUpdatesCache with parameters set to false.
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI for this database
	 * @throws CouchDBException		if CouchDB returns an error - response code >= 300. The response
	 * details are encapsulated in the exception.
	 */
	public void flushBulkUpdatesCache() throws IOException, URISyntaxException, CouchDBException {
		flushBulkUpdatesCache(false, false);
	}

	/**
	 * Returns the number of pending documents for bulk save in the internal cache
	 */
	public synchronized int numDocsPendingBulkUpdate() {
		return bulkUpdatesCache.size();
	}

	/**
	 * Removes all the documents pending bulk update
	 */
	public synchronized void clearBulkUpdatesCache() {
		bulkUpdatesCache.clear();
	}

	/**
	 * Retrieve information about a design document
	 * @see <a href="http://wiki.apache.org/couchdb/HTTP_view_API#Getting_Information_about_Design_Documents_.28and_their_Views.29">
	 * CouchDB Wiki for a description of the information contained in the returned JSON</a>
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI for this database
	 * @throws CouchDBException		if CouchDB returns an error - response code >= 300. The response
	 * details are encapsulated in the exception.
	 */
	public JsonNode designDocumentInfo(String designDocName) throws IOException, URISyntaxException, CouchDBException {
		return server.getHttpClient().get(URITemplates.designDocInfo(this, designDocName));
	}

	/**
	 * Retrieve documents from a view, filtered by the specified parameters.
	 * @param designDocName	name of the design document that contains the code for the view
	 * @param viewName		name of the view within the design document
	 * @param params		query parameters, e.g. startkey, endkey, limit, etc.
	 * @see <a href="http://wiki.apache.org/couchdb/HTTP_view_API#Querying_Options">CouchDB Wiki
	 * for a complete description of valid parameters</a>
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI for this database
	 * @throws CouchDBException		if CouchDB returns an error - response code >= 300. The response
	 * details are encapsulated in the exception.
	 */
	public ArrayList<Document> queryView(String designDocName, String viewName, List<NameValuePair> params)
			throws IOException, URISyntaxException, CouchDBException
	{
		java.net.URI uri = URITemplates.queryView(this,
												  designDocName,
												  viewName,
												  URLEncodedUtils.format(params, "utf-8"));
		JsonNode json = server.getHttpClient().get(uri);
		return parseDocuments(json);
	}

	/**
	 * Query a view and return the raw JSON.
	 * Use this instead of {@link #queryView(java.lang.String, java.lang.String, java.util.List) queryView} method
	 * if you prefer to receive metadata and/or process the results yourself and not have JZBoy create documents
	 * for you.
	 * For example, to retrieve the metadata for a view, such as the number of documents in it,  but not any documents,
	 * use this method and pass limit=0 in the params.
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI for this database
	 * @throws CouchDBException		if CouchDB returns an error - response code >= 300. The response
	 * details are encapsulated in the exception.
	 */
	public JsonNode queryViewRaw(String designDocName, String viewName, List<NameValuePair> params)
			throws IOException, URISyntaxException, CouchDBException
	{
		java.net.URI uri = URITemplates.queryView(this,
												  designDocName,
												  viewName,
												  URLEncodedUtils.format(params, "utf-8"));
		return server.getHttpClient().get(uri);
	}

	/**
	 * Retrieve rows from a view, whose keys match the specified keys.
	 * @param designDocName	name of the design document that contains the code for the view
	 * @param viewName		name of the view within the design document
	 * @param keys			the keys to retrieve. If the collection is ordered (e.g. a List), then
	 * the results match this order.
	 * @param params		query parameters, e.g. include_docs
	 * @return see {@link #queryView(java.lang.String, java.lang.String, java.util.List) queryView}
	 * for information on the result structure.
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI for this database
	 * @throws CouchDBException		if CouchDB returns an error - response code >= 300. The response
	 * details are encapsulated in the exception.
	 */
	public ArrayList<Document> getFromView(String designDocName,
										   String viewName,
										   Collection<String> keys,
										   List<NameValuePair> params)
								throws IOException, URISyntaxException, CouchDBException
	{
		// build a json document with the requested keys
		ObjectNode root = JsonNodeFactory.instance.objectNode();
		ArrayNode keysNode = root.putArray("keys");
		for (String key : keys)
			keysNode.add(key);
		String data = JsonUtils.serializeJson(root);
		java.net.URI uri = URITemplates.queryView(this, 
												  designDocName,
												  viewName,
												  URLEncodedUtils.format(params, "utf-8"));
		JsonNode json = server.getHttpClient().post(uri, data);
		return parseDocuments(json);
	}

	/**
	 * Run an ad-hoc view
	 * @param viewJsonStr	the view code
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI for this database
	 * @throws CouchDBException		if CouchDB returns an error - response code >= 300. The response
	 * details are encapsulated in the exception.
	 */
	public ArrayList<Document> tempView(String viewJsonStr) throws IOException, URISyntaxException, CouchDBException {
		String encoded = JsonUtils.reencodeUtf8ToIso88591(viewJsonStr);
		JsonNode json = server.getHttpClient().post(URITemplates.tempView(this), encoded);
		return parseDocuments(json);
	}

	/**
	 * Run an ad-hoc view
	 * @param viewJson	the view code as a JSON object
	 */
	public ArrayList<Document> tempView(JsonNode viewJson) throws IOException, URISyntaxException, CouchDBException {
		return tempView(JsonUtils.serializeJson(viewJson));
	}

	/**
	 * Compact the views in the specified design document.
	 * @param designDocName		name the design document, excluding the '_design/' prefix
	 * @see <a href="http://wiki.apache.org/couchdb/Compaction#View_compaction">CouchDB Wiki</a>
 	 * @throws IOException			if the HttpClient throws an IOException
	 * @throws URISyntaxException	if there was a problem constructing a URI for this database
	 * @throws CouchDBException		if CouchDB returns an error - response code >= 300. The response
	 * details are encapsulated in the exception.
	 */
	public void compactViews(String designDocName) throws IOException, URISyntaxException, CouchDBException  {
		server.getHttpClient().post(URITemplates.compactView(this, designDocName));
	}

	/**
	 * Remove outdated view indexes that remain on the disk.
	 * @see <a href="http://wiki.apache.org/couchdb/Compaction#View_compaction">CouchDB Wiki</a>
	 */
	public void cleanupViews() throws IOException, URISyntaxException, CouchDBException  {
		server.getHttpClient().post(URITemplates.cleanupViews(this));
	}
	
	@Override
	public String toString() {
		return this.server.toString() + this.dbName;
	}
	
}
