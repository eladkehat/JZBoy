JZBoy
=====

JZBoy is the most simple way to work with [CouchDB][0] from Java.

Rather than provide a full-fledged persistence layer, it lets you work with
CouchDB directly via JSON (using [Jackson][1]).
This makes it fast and simple to use, adding as few lines as possible to your code base.


Use Cases
---------

JZBoy is perfect for when you don't need to map Java objects to JSON / CouchDB documents.
Some use cases include:

 * Caching the results of web APIs that already speak JSON (e.g. Twitter)
 * Storing opaque attachments


Dependencies
------------

JZBoy is built on a minimal set of external libraries:

* [Apache HttpComponents][2]
* [Jackson][1]
* [SLF4J][3]


Example
-------

Create a `Database`:

``` java
// database called "my-db" on http://127.0.0.1:5984
Database db = new Database("my-db");
// or database called "my-db" on http://mydomain.com:5984
Database db = new Database("mydomain.com", 5984, "my-db");
```

Ensure that the database exists on your CouchDB server:

``` java
db.createIfNotExists();
```

Create a new document:

``` java
String uuid = db.getServer().nextUUID();
String json = "{\"field1\":\"value1\",\"field2\":2}"
// if the last parameter was, it would add the document in batch mode
Document res = db.createDocument(uuid, json, false);
```

The result encapsulates CouchDB's response:

``` java
// prints the uuid
System.out.println(res.getId());
// prints the revision assigned to the new document
System.out.println(res.getRev());
// CouchDB returned a JSON with ok=true:
System.out.println("ok=" + res.getJson().get("ok").getBooleanValue());
```

Now retrieve the document we just saved, and update it:

``` java
Document doc = db.getDocument(uuid);
// the returned document object wraps the id, revision, and the JSON content
String newJson = "{\"field1\":1,\"field2\":\"value2\"}";
doc.setJson(newJson);
db.updateDocument(doc);
```

Here's how to query a view:

``` java
// create some query parameters
List<NameValuePair> params = new ArrayList<NameValuePair>() {{
	add(new BasicNameValuePair("include_docs", "true"));
	add(new BasicNameValuePair("startkey", "\"abcde\""));
}};
List<Document> docs = db.queryView("my-design-doc-name", "my-view", params);
for (Document doc : docs) {
	System.out.println(doc.getJson());
}
```


Building from Source
--------------------

JZBoy uses ant and {Ivy}[http://ant.apache.org/ivy/] for dependency resolution.
If you're unfamiliar with Ivy, follow instructions {here}[http://ant.apache.org/ivy/history/latest-milestone/install.html]
or just download and drop ivy.jar into your ant lib directory. It will download dependencies from the standard
Maven2 repository when you run ant.


[0]: http://couchdb.apache.org/
[1]: http://wiki.fasterxml.com/JacksonHome
[2]: http://hc.apache.org/
[3]: http://www.slf4j.org/
