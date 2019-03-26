This needs a pre-configured table in BigQuery with the following schema:
<pre>
ReceivedTimestamp	TIMESTAMP	NULLABLE
Client	STRING	NULLABLE
ClientTimestamp	TIMESTAMP	NULLABLE
ClientHostname	STRING	NULLABLE
Tag	STRING	NULLABLE
Message	STRING	NULLABLE
Priority	INTEGER	NULLABLE
Severity	INTEGER	NULLABLE
Facility	INTEGER	NULLABLE
</pre>

...in addition to all the usual shenanigans with setting up a GCP project, enabling billing, configuring a BigQuery dataset, and setting up a service account with credentials that this program can use.
