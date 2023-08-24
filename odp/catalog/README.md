# ODP Catalog

*Twice a day, at 2pm and 2am, the ODP catalog service is kicked into
action by cron, in a container running on a Linux host somewhere in
the uLwazi outback.*

## The ODP publishing method

The publishing method is applied to each implemented ODP catalog
(currently SAEON, MIMS and DataCite) in turn. Its function is to
ensure that the set of published records accessible at a catalog's
API endpoint is up to date with respect to the state of all ODP
collections, records and tags.

First, a temporary snapshot is created, consisting of record API
output for all ODP records with recently updated timestamps. Record
timestamps are updated whenever any changes are made to digital object
metadata or identifiers, and also every time any referencing object such
as a tag or a child record is added, updated or removed. The record API
output model consists of digital object metadata and identifiers, parent
and child record references (if any), and associated record and collection
tags. To ensure consistency of lookup information across all of a catalog's
published records, the database transaction isolation level is set to
'REPEATABLE READ' while taking the snapshot.

Next, each record API output object in the snapshot is evaluated against
the catalog's rules for deciding whether or not the record should be
published to that catalog. For records that should be published, a
catalog-specific representation of the record's digital object metadata,
identifiers and auxiliary information is created (or updated), and indexing
data are updated to facilitate full-text, spatial, temporal and faceted search.
Records that should not be published are made accessible as retracted record
stubs at the catalog's API endpoint, to facilitate local deletion by
external catalog client systems.

Finally, in the case of an external catalog system such as DataCite, to
which the ODP is a client, the published records for the catalog are
mirrored to that catalog system using its own API.
