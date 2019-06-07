# PostgresCatalog.jl


## Overview

`PostgresCatalog` is a Julia library for introspecting Postgres databases and
generating models of the database structure.


### Installation

Use the Julia package manager.

```julia
julia> using Pkg
julia> Pkg.add("PostgresCatalog")
```


### Usage Guide

We will demonstrate how to use `PostgresCatalog` on a sample database schema containing
a single table ``individual``.

    using LibPQ

    conn = LibPQ.Connection("")

    execute(conn, "BEGIN")

    execute(conn,
            """
            CREATE TYPE individual_sex_enum AS ENUM ('male', 'female', 'other', 'unknown');

            CREATE TABLE individual (
                id int4 NOT NULL,
                mrn text NOT NULL,
                sex individual_sex_enum NOT NULL DEFAULT 'unknown',
                mother_id int4,
                father_id int4,
                CONSTRAINT individual_uk UNIQUE (id),
                CONSTRAINT individual_pk PRIMARY KEY (mrn),
                CONSTRAINT individual_mother_fk FOREIGN KEY (mother_id) REFERENCES individual (id),
                CONSTRAINT individual_father_fk FOREIGN KEY (father_id) REFERENCES individual (id)
            );
            """)

To generate a model of the database structure, we use function [`PostgresCatalog.introspect`](@ref),
which returns a [`PostgresCatalog.PGCatalog`](@ref) object.

    using PostgresCatalog

    cat = PostgresCatalog.introspect(conn)
    #-> DATABASE " … "

By traversing the catalog, we can obtain the models of database tables, which
are represented as [`PostgresCatalog.PGTable`](@ref) objects.

    scm = cat["public"]
    #-> SCHEMA "public"

    tbl = scm["individual"]
    #-> TABLE "individual"

The table model contains information about its columns in the form of
[`PostgresCatalog.PGColumn`](@ref) objects.

    foreach(println, tbl)
    #=>
    COLUMN "individual"."id" "int4" NOT NULL
    COLUMN "individual"."mrn" "text" NOT NULL
    COLUMN "individual"."sex" "individual_sex_enum" NOT NULL
    COLUMN "individual"."mother_id" "int4" NULL
    COLUMN "individual"."father_id" "int4" NULL
    =#

Properties of a table column are available as attributes on the corresponding
model object.

    col = tbl["sex"]
    #-> COLUMN "individual"."sex" "individual_sex_enum" NOT NULL

    col.name
    #-> "sex"

    col.type_
    #-> TYPE "individual_sex_enum"

    col.not_null
    #-> true

    col.default
    #-> "'unknown'::individual_sex_enum"

Description of unique and foreign key constraints defined on the table is also
available in the form of [`PostgresCatalog.PGUniqueKey`](@ref) and
[`PostgresCatalog.PGForeignKey`](@ref) objects.

    tbl.primary_key
    #-> CONSTRAINT "individual"."individual_pk" PRIMARY KEY ("mrn")

    foreach(println, tbl.unique_keys)
    #=>
    CONSTRAINT "individual"."individual_pk" PRIMARY KEY ("mrn")
    CONSTRAINT "individual"."individual_uk" UNIQUE ("id")
    =#

    foreach(println, tbl.foreign_keys)
    #=>
    CONSTRAINT "individual"."individual_father_fk" FOREIGN KEY ("father_id") REFERENCES "individual" ("id")
    CONSTRAINT "individual"."individual_mother_fk" FOREIGN KEY ("mother_id") REFERENCES "individual" ("id")
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

