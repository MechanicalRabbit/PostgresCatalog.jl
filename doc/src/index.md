# PostgresCatalog.jl

PostgresCatalog is a Julia library for introspecting Postgres databases and
generating models of the database structure.  It provides information about
database schemas, types, tables, columns, unique and foreign key constraints.

Ability to modify the database structure will be added in a future release.


## Installation

Use the Julia package manager.

```julia
julia> using Pkg
julia> Pkg.add("PostgresCatalog")
```


## Usage Guide

To demonstrate PostgresCatalog, we create a database containing just one table.

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

Function [`PostgresCatalog.introspect`](@ref) generates a
[`PostgresCatalog.PGCatalog`](@ref) object containing a model of this database.

    using PostgresCatalog

    cat = PostgresCatalog.introspect(conn)
    #-> DATABASE " … "

By traversing the catalog, we can obtain the table model represented by a
[`PostgresCatalog.PGTable`](@ref) object.

    scm = cat["public"]
    #-> SCHEMA "public"

    tbl = scm["patient"]
    #-> TABLE "patient"

The table owns column models, which are represented by
[`PostgresCatalog.PGColumn`](@ref) objects.

    foreach(println, tbl)
    #=>
    COLUMN "patient"."id" "int4" NOT NULL
    COLUMN "patient"."mrn" "text" NOT NULL
    COLUMN "patient"."sex" "patient_sex_enum" NOT NULL
    COLUMN "patient"."mother_id" "int4" NULL
    COLUMN "patient"."father_id" "int4" NULL
    =#

Column properties can be discovered through model attributes.

    col = tbl["sex"]
    #-> COLUMN "patient"."sex" "patient_sex_enum" NOT NULL

    col.name
    #-> "sex"

    col.type
    #-> TYPE "patient_sex_enum"

    col.type.labels
    #-> ["male", "female", "other", "unknown"]

    col.not_null
    #-> true

    col.default
    #-> "'unknown'::patient_sex_enum"

The table also owns the models of its unique and foreign key constraints.

    tbl.primary_key
    #-> CONSTRAINT "patient"."patient_pk" PRIMARY KEY ("mrn")

    tbl.primary_key.name
    #-> "patient_pk"

    foreach(println, tbl.primary_key.columns)
    #-> COLUMN "patient"."mrn" "text" NOT NULL

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

