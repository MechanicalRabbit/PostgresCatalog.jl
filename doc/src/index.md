# PostgresCatalog.jl


## Overview

`PostgresCatalog` is a Julia library for introspecting and manipulating the
structure of a Postgres database.


### Installation

Use the Julia package manager.

```julia
julia> using Pkg
julia> Pkg.add("PostgresCatalog")
```


### Using `PostgresCatalog`

Before we can use `PostgresCatalog`, we need to create a database connection,
which is done with the `LibPQ` library.

    using LibPQ

    conn = LibPQ.Connection("")

We encapsulate all interactions with the database in a transaction.

    execute(conn, "BEGIN")

To start, we generate a *catalog* object, which contains the information about
the database structure.

    using PostgresCatalog

    cat = PostgresCatalog.introspect(conn)
    #-> <DATABASE>

As this is a newly created database, it only contains the system `pg_catalog` schema
an empty `public` schema.

    sys = cat["pg_catalog"]
    #-> <SCHEMA "pg_catalog">

    scm = cat["public"]
    #-> <SCHEMA "public">

    isempty(scm)
    #-> true

It is easy to create a new table.

    patient_tbl = scm.create_table!(conn,
                                    "patient",
                                    [(name = "id", type_ = sys.types["int4"], not_null = true),
                                     (name = "mrn", type_ = sys.types["text"], not_null = true)])


## API Reference

```@docs
PostgresCatalog.introspect
```


## Test Suite

