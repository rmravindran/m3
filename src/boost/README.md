# General Info

This package is meant to be placed as a standalone package (tsboost).
This is a tentative placement and might still be moved out of m3db based on how
the development proceeds.

## Core Concepts

### Time-evolving

Boost takes the "0-schema" approach. Meaning the users of the database are not
required to maintain any schema on their side. All things are considered
**real-time, time-parametrized**.

**Real-time**: All operations on the series are real-time. There is no "send
this update to a time in the past" feature. Latency and data-freshness spectrum
is meant to usable where milliseconds guarantees are necessary (based on the
hardware footprint)

**Time-parametrized**: All operations are time-depended and users does not
need to bear the burden of managing the time evolution of metadata or schema.
This includes even the composition of series family. For example, at **t0** you
can chose to have **cpu_utilization** and **mem_utilization** in the
**mywebapp_family** family. And then at **t1** you could chose a completely
different composition.

## Series Nomenclature

Boost has 3 user facing concepts, domain, series family and series. A series
name is fully qualified when all 3 specifications are present. While there are
defaults for domain name (dd) and series family (naf), they are nevertheless
required for accessing and interpretting the data of a series.

Internally, there is one more concept called *shard number* which is assigned
based on the distribution factor (user configurable parameter) of a series. A
series data may be distributed (imagine segments) among at most shards. For a
given latency profile, as the distribution factor increases, it provides more
read & write throughput at the expense of hardware footprint. **Note that the
distribution factor is also time-parametrized**.

### Fully Qualified Series Name
Series that contains user data is fully qualified as following:

```
 [DomainName]::[SeriesFamily]::dataXXXXX::[SeriesName]
```
 where
 - DomainName is user provided domain name (or a the default domain "dd")
- SeriesFamily is the series family of the series. If the series is not part of
a series family, the default family of "naf" (not a family) will be assigned.
- dataXXXXX, where the XXXXX contains a 5 digit shard id
- SeriesName is the user provided series name
 
### Series used for metadata

These are internal concepts not exposed to the users.

#### Symbol table metadata

Series that contains metadata will have different naming pattern.

Symbol table of a series that is part of a user defined series family will have
the following nomenclature:

```
 [DomainPrefix]::[SeriesFamily]::symboltable
```

Symbol table of a series that is part of the default series family will have
the following nomenclature:
```
 [DomainPrefix]::[SeriesFamily]::symboltable::[SeriesName]
```

## Attribute Specification

During ingestion, tags that are attribute as specified as "attr::[TagName]"
where the "attr::" prefix denotes the tag should be interpretted as an
attribute.

## Query Example

<code> SELECT attr::hostname, attr::value, cluster FROM
fundamentals.cpu_utilization WHERE zone="east1" AND time > "now-1d" </code>

- **hostname** is an attribute
- **value** is the time series value. Note that for value, attribute
specification is optional.
- **cluster** and **zone** are tags
- **time** is a reserved field for time parametrization
- **fundamentals** is a series family name
