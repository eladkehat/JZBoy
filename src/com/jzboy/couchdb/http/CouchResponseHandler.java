package com.jzboy.couchdb.http;

import java.io.IOException;

import org.apache.http.annotation.Immutable;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.HttpResponseException;
import org.apache.http.util.EntityUtils;

/**
 * A {@link org.apache.http.client.ResponseHandler ResponseHandler} that returns a CouchResponse based on
 * CouchDB API HTTP response.
 * 
 * @author Elad Kehat
 */
@Immutable
public class CouchResponseHandler implements ResponseHandler<CouchResponse> {

	/**
	 * Returns the response body as a String if the response was successful (a
	 * 2xx status code). If no response body exists, this returns null. If the
	 * response was unsuccessful (>= 300 status code), throws an
	 * {@link HttpResponseException}.
	 */
	@Override
	public CouchResponse handleResponse(final HttpResponse response) throws HttpResponseException, IOException {
		StatusLine statusLine = response.getStatusLine();
		String body = null;
		HttpEntity entity = response.getEntity();
		if (entity != null)
			body = EntityUtils.toString(entity, "utf-8");
		
		return new CouchResponse(statusLine.getStatusCode(), statusLine.getReasonPhrase(), body);
	}

}
