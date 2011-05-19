package com.jzboy.couchdb.http;

import com.jzboy.couchdb.CouchDBException;
import com.jzboy.couchdb.util.JsonUtils;
import java.io.IOException;
import java.net.URI;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.codehaus.jackson.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides methods that generate HTTP-REST requests to CouchDB APIs.
 * 
 * @author Elad Kehat
 */
public class CouchHttpClient {

	private static final Logger logger = LoggerFactory.getLogger(CouchHttpClient.class);
	
	final HttpClient httpclient = new DefaultHttpClient();

	/**
	 * Sends a GET request to the speficifed CouchDB endpoint and returns CouchDB's response as a JSON object.
	 * Use this method on APIs that return a JSON String, where you want that JSON parsed for you.
	 * @param uri	CouchDB API endpoint
	 * @return the JSON tree parsed form the response body
	 * @throws IOException		if the HttpClient throws an IOException
	 * @throws CouchDBException	if CouchDB returns an error - response code >= 300. The response details are
	 * encapsulated in the exception.
	 */
	public JsonNode get(URI uri) throws IOException, CouchDBException {
		HttpGet req = new HttpGet(uri);
		return execRequest(req);
	}

	/**
	 * Sends a GET request to the speficifed CouchDB endpoint and returns the response body as a String
	 * @param uri	CouchDB API endpoint
	 * @return the response body, as a String
	 * @throws IOException		if the HttpClient throws an IOException
	 * @throws CouchDBException	if CouchDB returns an error - response code >= 300. The response details are
	 * encapsulated in the exception.
	 */
	public String getBody(URI uri) throws IOException, CouchDBException {
		HttpGet req = new HttpGet(uri);
		CouchResponse res = httpclient.execute(req, new CouchResponseHandler());
		if (res.getStatusCode() >= 300) {
			throw new CouchDBException(res.getStatusCode(), res.getBody());
		}
		return res.getBody();
	}

	/**
	 * Sends a GET request to the speficifed CouchDB endpoint and returns the raw HttpResponse object.
	 * Make sure you consume the entire content in HttpResponse, so that the connection gets released.
	 * @param uri	CouchDB API endpoint
	 * @return the raw HttpRepsonse, untouched
	 * @throws IOException		if the HttpClient throws an IOException
	 */
	public HttpResponse getHttpResponse(URI uri) throws IOException {
		HttpGet req = new HttpGet(uri);
		return httpclient.execute(req);
	}

	/**
	 * Sends a GET request to the speficifed CouchDB endpoint and returns a CouchResponse object.
	 * Use this method if you want to avoid the default handling of errors and parsing to JSON.
	 * @param uri	CouchDB API endpoint
	 * @return the response, as a CouchResponse
	 * @throws IOException		if the HttpClient throws an IOException
	 */
	public CouchResponse getCouchResponse(URI uri) throws IOException {
		HttpGet req = new HttpGet(uri);
		return httpclient.execute(req, new CouchResponseHandler());
	}

	/**
	 * Sends a POST request to the specified CouchDB endpoint.
	 * @param uri	CouchDB API endpoint
	 * @param data	request data
	 * @return the JSON tree parsed form the response body
	 * @throws IOException		if the HttpClient throws an IOException
	 * @throws CouchDBException	if CouchDB returns an error - response code >= 300. The response details are
	 * encapsulated in the exception.
	 */
	public JsonNode post(URI uri, String data) throws IOException, CouchDBException {
		HttpPost req = new HttpPost(uri);
		if (data != null)
			req.setEntity(new StringEntity(data));
		return execRequest(req);
	}

	/**
	 * Sends a POST request with no data to the specified CouchDB endpoint.
	 * @param uri	CouchDB API endpoint
	 * @return the JSON tree parsed form the response body
	 * @throws IOException		if the HttpClient throws an IOException
	 * @throws CouchDBException	if CouchDB returns an error - response code >= 300. The response details are
	 * encapsulated in the exception.
	 */
	public JsonNode post(URI uri) throws IOException, CouchDBException {
		return post(uri, null);
	}

	/**
	 * Sends a PUT request to the specified CouchDB endpoint.
	 * If the request fails due to a socket exception, e.g. the server refuses the request, it is retried several times.
	 * @param uri	CouchDB API endpoint
	 * @param data	request data
	 * @param retry maximum number of retries if the request fails
	 * @return the JSON tree parsed form the response body
	 * @throws IOException		if the HttpClient throws an IOException
	 * @throws CouchDBException	if CouchDB returns an error - response code >= 300. The response details are
	 * encapsulated in the exception.
	 */
	public JsonNode put(URI uri, String data, int retry) throws IOException, CouchDBException {
		HttpPut req = new HttpPut(uri);
		if (data != null)
			req.setEntity(new StringEntity(data));
		return execRequest(req, retry);
	}

	/**
	 * Sends a PUT request to the specified CouchDB endpoint.
	 * @param uri	CouchDB API endpoint
	 * @param data	request data
	 * @return the JSON tree parsed form the response body
	 * @throws IOException		if the HttpClient throws an IOException
	 * @throws CouchDBException	if CouchDB returns an error - response code >= 300. The response details are
	 * encapsulated in the exception.
	 */
	public JsonNode put(URI uri, String data) throws IOException, CouchDBException {
		HttpPut req = new HttpPut(uri);
		if (data != null)
			req.setEntity(new StringEntity(data));
		return execRequest(req);
	}

	/**
	 * Sends a PUT request with no data to the specified CouchDB endpoint.
	 * @param uri	CouchDB API endpoint
	 * @return the JSON tree parsed form the response body
	 * @throws IOException		if the HttpClient throws an IOException
	 * @throws CouchDBException	if CouchDB returns an error - response code >= 300. The response details are
	 * encapsulated in the exception.
	 */
	public JsonNode put(URI uri) throws IOException, CouchDBException {
		return put(uri, null);
	}

	/**
	 * Sends a PUT request to the specified CouchDB endpoint.
	 * Use this version to add attachments.
	 * @param uri		CouchDB API endpoint
	 * @param data		request data
	 * @param mimeType	the MIME type of the data. This is added as a Content-Type header to the request
	 * @return the JSON tree parsed form the response body
	 * @throws IOException		if the HttpClient throws an IOException
	 * @throws CouchDBException	if CouchDB returns an error - response code >= 300. The response details are
	 * encapsulated in the exception.
	 */
	public JsonNode put(URI uri, byte[] data, String mimeType) throws IOException, CouchDBException {
		HttpPut req = new HttpPut(uri);
		req.setEntity(new ByteArrayEntity(data));
		req.addHeader("Content-Type", mimeType);
		return execRequest(req);
	}

	/**
	 * Sends a DELETE request to the specified CouchDB endpoint.
	 * @param uri	CouchDB API endpoint
	 * @return the JSON tree parsed form the response body
	 * @throws IOException		if the HttpClient throws an IOException
	 * @throws CouchDBException	if CouchDB returns an error - response code >= 300. The response details are
	 * encapsulated in the exception.
	 */
	public JsonNode delete(URI uri) throws IOException, CouchDBException {
		HttpDelete req = new HttpDelete(uri);
		return execRequest(req);
	}

	private JsonNode execRequest(HttpRequestBase req, int retry) throws IOException, CouchDBException {
		int attemptsLeft = retry;
		CouchResponse res = null;
		while (res == null && attemptsLeft > 0) {
			try {
				res = httpclient.execute(req, new CouchResponseHandler());
			} catch (java.net.SocketException se) {
				logger.debug("Got {} on update attempt #{} on {}",
							 new Object[] {se.getMessage(), (3-attemptsLeft+1), req.getURI()});
				attemptsLeft--;
			}
		}
		if (res == null)
			throw new CouchDBException(String.format("Operation failed after %d attempts: %s", retry, req.getURI()));
		throwOnError(res);
		return res.getBodyAsJson();
	}

	private JsonNode execRequest(HttpRequestBase req) throws IOException, CouchDBException {
		CouchResponse res = httpclient.execute(req, new CouchResponseHandler());
		throwOnError(res);
		return res.getBodyAsJson();
	}

	private void throwOnError(CouchResponse res) throws IOException, CouchDBException {
			if (res.getStatusCode() >= 300) {
				throw new CouchDBException(res.getStatusCode(), buildErrorMessageFromResponse(res));
			}
	}

	private static String buildErrorMessageFromResponse(final CouchResponse res) throws IOException {
		return new StringBuilder(JsonUtils.serializeError(res.getBodyAsJson())).
						append("(").append(res.getStatusCode()).append(")").toString();
	}

}
