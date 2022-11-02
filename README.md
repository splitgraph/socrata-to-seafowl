# Socrata-to-Seafowl sync job

This repository contains the code used to sync the data from
[our index of open data on Socrata](https://www.splitgraph.com/splitgraph/socrata)
into a [Seafowl](https://github.com/splitgraph/seafowl) instance.

This data will power the [SocFeed app](https://socfeed.vercel.app) in the future.

In the meantime, see the [Observable notebook](https://observablehq.com/@seafowl/socrata)
that showcases this dataset.

## How it works

- Every night (currently on-demand), we initiate a download of the new snapshots of
  [Socrata's Discovery API](https://socratadiscovery.docs.apiary.io/) from
  Splitgraph in the Parquet format
- This gives us a pre-signed S3 URL to download the file
- We use [`CREATE EXTERNAL TABLE`](https://seafowl.io/docs/guides/csv-parquet-http-external) 
  on Seafowl with this URL to append this data to a history table (bypassing having to download
  this file from the GitHub Actions instance)
- Then, we use a [not dbt](https://github.com/splitgraph/socrata-to-seafowl/blob/master/src/s2sf/notdbt.py) script
  that creates some derived tables (monthly/weekly/daily summary) used by the [SocFeed app](https://socfeed.vercel.app)
  (actual dbt support coming soon!)
