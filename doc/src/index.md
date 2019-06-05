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

To demonstrate `PostgresCatalog`, we need to create a sample database schema.

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

Now we can generate a *catalog* object, which contains the information about
the database structure.

    using PostgresCatalog

    cat = PostgresCatalog.introspect(conn)
    #-> DATABASE " … "

In this catalog object, we can find the table `invididual` in the `public` schema.

    scm = cat["public"]
    #-> SCHEMA "public"

    tbl = scm["individual"]
    #-> TABLE "individual"

For this table, we lists its columns.

    foreach(println, tbl)
    #=>
    COLUMN "individual"."id" "int4" NOT NULL
    COLUMN "individual"."mrn" "text" NOT NULL
    COLUMN "individual"."sex" "individual_sex_enum" NOT NULL
    COLUMN "individual"."mother_id" "int4" NULL
    COLUMN "individual"."father_id" "int4" NULL
    =#

We can also access individual columns and their attributes.

    col = tbl["sex"]

    col.name        #-> "sex"

    col.type_       #-> TYPE "individual_sex_enum"

    col.not_null    #-> true

    col.default     #-> "'unknown'::individual_sex_enum"

We can find the constraints defined on this table.

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
```

