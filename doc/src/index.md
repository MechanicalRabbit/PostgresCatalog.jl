# PostgresCatalog.jl


## Overview

PostgresCatalog is a Julia library for introspecting Postgres databases and
generating models of the database structure.


### Installation

Use the Julia package manager.

```julia
julia> using Pkg
julia> Pkg.add("PostgresCatalog")
```


### Usage Guide

We will demonstrate how to use `PostgresCatalog` on a sample database schema
containing just one table.

    using LibPQ

    conn = LibPQ.Connection("")

    execute(conn, "BEGIN")

    execute(conn,
            """
            CREATE TYPE patient_sex_enum AS ENUM ('male', 'female', 'other', 'unknown');

            CREATE TABLE patient (
                id int4 NOT NULL,
                mrn text NOT NULL,
                sex patient_sex_enum NOT NULL DEFAULT 'unknown',
                mother_id int4,
                father_id int4,
                CONSTRAINT patient_uk UNIQUE (id),
                CONSTRAINT patient_pk PRIMARY KEY (mrn),
                CONSTRAINT patient_mother_fk FOREIGN KEY (mother_id) REFERENCES patient (id),
                CONSTRAINT patient_father_fk FOREIGN KEY (father_id) REFERENCES patient (id)
            );
            """)

To generate a model of the database structure, we use function
[`PostgresCatalog.introspect`](@ref), which returns a
[`PostgresCatalog.PGCatalog`](@ref) object.

    using PostgresCatalog

    cat = PostgresCatalog.introspect(conn)
    #-> DATABASE " … "

By traversing the catalog, we can obtain the models of database tables, which
are represented as [`PostgresCatalog.PGTable`](@ref) objects.

    scm = cat["public"]
    #-> SCHEMA "public"

    tbl = scm["patient"]
    #-> TABLE "patient"

The table model contains information about its columns in the form of
[`PostgresCatalog.PGColumn`](@ref) objects.

    foreach(println, tbl)
    #=>
    COLUMN "patient"."id" "int4" NOT NULL
    COLUMN "patient"."mrn" "text" NOT NULL
    COLUMN "patient"."sex" "patient_sex_enum" NOT NULL
    COLUMN "patient"."mother_id" "int4" NULL
    COLUMN "patient"."father_id" "int4" NULL
    =#

Properties of a table column are available as attributes on the corresponding
model object.

    col = tbl["sex"]
    #-> COLUMN "patient"."sex" "patient_sex_enum" NOT NULL

    col.name
    #-> "sex"

    col.type_
    #-> TYPE "patient_sex_enum"

    col.not_null
    #-> true

    col.default
    #-> "'unknown'::patient_sex_enum"

Description of unique and foreign key constraints defined on the table is also
available in the form of [`PostgresCatalog.PGUniqueKey`](@ref) and
[`PostgresCatalog.PGForeignKey`](@ref) objects.

    tbl.primary_key
    #-> CONSTRAINT "patient"."patient_pk" PRIMARY KEY ("mrn")

    foreach(println, tbl.unique_keys)
    #=>
    CONSTRAINT "patient"."patient_pk" PRIMARY KEY ("mrn")
    CONSTRAINT "patient"."patient_uk" UNIQUE ("id")
    =#

    foreach(println, tbl.foreign_keys)
    #=>
    CONSTRAINT "patient"."patient_father_fk" FOREIGN KEY ("father_id") REFERENCES "patient" ("id")
    CONSTRAINT "patient"."patient_mother_fk" FOREIGN KEY ("mother_id") REFERENCES "patient" ("id")
    =#


## API Reference

```@docs
PostgresCatalog.introspect
PostgresCatalog.PGCatalog
PostgresCatalog.PGSchema
PostgresCatalog.PGType
PostgresCatalog.PGTable
PostgresCatalog.PGColumn
PostgresCatalog.PGUniqueKey
PostgresCatalog.PGForeignKey
```

