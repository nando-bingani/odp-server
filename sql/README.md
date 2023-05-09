# SQL directory info

The `.sql` files in this directory hierarchy contain SQL `EXPLAIN` statements for
queries produced in ODP code by SQLAlchemy. The queries can be reproduced during a
debug session by evaluating `str(stmt)` where `stmt` is, for example, a SQLAlchemy
`select` construct.

Column selections have been reduced wherever possible for brevity (e.g. to `SELECT 1`),
since this affects only the `width` shown in execution plans, not the algorithm or the
cost.

Using `EXPLAIN` is essential in deciding whether and how to define indexes on tables.
For example, we might want to create an index to eliminate an expensive `Seq Scan`
(full table scan) or to reduce the filtering that needs to be done by a `Bitmap Heap Scan`
at some level in an execution plan. Run any relevant `EXPLAIN` statements both before
and after creating the index, and do a side-by-side comparison of execution plans, to
see if the index is actually used and makes a difference to the execution cost.

## Useful references

* https://use-the-index-luke.com/
* https://www.postgresql.org/docs/14/using-explain.html
