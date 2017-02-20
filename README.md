# Ruminant

`Ruminant`queries an ElasticSearch database, processes the results and feeds them
as time series to an Influx database. ETL for a rather specific use case, basically.

## But how?

Processing data with `Ruminant` performs a few steps that you should understand:

**Find out where to start:** First the targeted time series in the Influx 
Database is queried in for its last _marker timestamp_. This timestamp indicates
at what time the last run of `Ruminant` was performed and is used as starting
point for this run...

**Fetch the data from ElasticSearch:** A query provided is executed and its result
is prepared to be processed. A query can be executed in two manners:

1. If no _sampler configuration_ is provided, the query is executed once. This is
kind of execution is a good fit if you can extract timestamps for your time series
from the results of your ElasticSeach query, eg. if the query performs a `date_histogram`
aggregation for example.
2. If a query does not contain a `date_histogram` aggregation and needs to be 
executed once per point in your time series, a _sampler configuration_ can be passed.
This allows to run the same query multiple times with incrementing timestamps.

This step in known as _regurgitate_ in the ruminant jargon.

**Process the results and build time series:** ElasticSearch returns the resuls
of the query as JSON data. with simple expressions, `Ruminant` allows you to
iterate over these results and lets you indicate where in the JSON information
can be found that should be stored with the time series.

This step in known as _ruminate_ in the ruminant jargon.

**Persist time series:** The set of data points created in the last step is then
saved to the Influx database and series specified in your configuration. Also, a
new _marker timestamp_ is written that indicates the the new latest point in your
series to indicate where to start on the next run.

This step in known as _gulp_ in the ruminant jargon.

![How It Works](https://raw.githubusercontent.com/unprofession-al/ruminant/master/ruminant.png "How it works")
