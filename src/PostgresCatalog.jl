module PostgresCatalog

using LibPQ

mutable struct PGType
    name::String
    lbls::Union{Vector{String},Missing}

    PGType(name, lbls=missing) =
        new(name, lbls)
end

mutable struct PGColumn
    name::String
    typ::PGType
    nn::Bool
end

mutable struct PGTable
    name::String
    comment::Union{String,Missing}
    cols::Vector{PGColumn}

    PGTable(name) =
        new(name, missing, PGColumn[])
end

mutable struct PGSchema
    name::String
    comment::Union{String,Missing}
    name2type::Dict{String,PGType}
    name2table::Dict{String,PGTable}

    PGSchema(name) =
        new(name, missing, Dict{String,PGType}(), Dict{String,PGTable}())
end

mutable struct PGCatalog
    name2schema::Dict{String,PGSchema}

    PGCatalog() =
        new(Dict{String,PGSchema}())
end

function add_schema(cat::PGCatalog, name)
    scm = PGSchema(name)
    cat.name2schema[name] = scm
    scm
end

add_schema(name) =
    cat -> add_schema(cat, name)

function add_type(scm::PGSchema, name)
    typ = PGType(name)
    scm.name2type[name] = typ
    typ
end

add_type(name) =
    scm -> add_type(scm, name)

function add_enum_type(scm::PGSchema, name, lbls)
    typ = PGType(name, lbls)
    scm.name2type[name] = typ
    typ
end

add_enum_type(name, lbls) =
    scm -> add_enum_type(scm, name, lbls)

function add_table(scm::PGSchema, name)
    tbl = PGTable(name)
    scm.name2table[name] = tbl
    tbl
end

add_table(name) =
    scm -> add_table(scm, name)

function add_column(tbl, name, typ, nn)
    col = PGColumn(name, typ, nn)
    push!(tbl.cols, col)
    col
end

add_column(name, typ, nn) =
    tbl -> add_column(tbl, name, typ, nn)

function introspect(conn)
    cat = PGCatalog()

    # Extract schemas.
    oid2schema = Dict{UInt32,PGSchema}()
    res = execute(conn, """
        SELECT n.oid, n.nspname
        FROM pg_catalog.pg_namespace n
        ORDER BY n.nspname
    """)
    foreach(zip(fetch!(NamedTuple, res)...)) do (oid, nspname)
        scm = cat |> add_schema(nspname)
        oid2schema[oid] = scm
    end

    # Extract ENUM labels.
    oid2labels = Dict{UInt32,Vector{String}}()
    res = execute(conn, """
        SELECT e.enumtypid, e.enumlabel
        FROM pg_catalog.pg_enum e
        ORDER BY e.enumtypid, e.enumsortorder, e.oid
    """)
    foreach(zip(fetch!(NamedTuple, res)...)) do (enumtypid, enumlabel)
        lbls = get!(() -> String[], oid2labels, enumtypid)
        push!(lbls, enumlabel)
    end

    # Extract data types.
    oid2type = Dict{UInt32,PGType}()
    res = execute(conn, """
        SELECT t.oid, t.typnamespace, t.typname, t.typtype
        FROM pg_catalog.pg_type t
        ORDER BY t.typnamespace, t.typname
    """)
    foreach(zip(fetch!(NamedTuple, res)...)) do (oid, typnamespace, typname, typtype)
        scm = oid2schema[typnamespace]
        typ =
            if typtype == "e"
                scm |> add_enum_type(typname, oid2labels[oid])
            else
                scm |> add_type(typname)
            end
        oid2type[oid] = typ
    end

    # Extract tables.
    oid2table = Dict{UInt32,PGTable}()
    res = execute(conn, """
        SELECT c.oid, c.relnamespace, c.relname
        FROM pg_catalog.pg_class c
        WHERE c.relkind IN ('r', 'v') AND
              HAS_TABLE_PRIVILEGE(c.oid, 'SELECT')
        ORDER BY c.relnamespace, c.relname
    """)
    foreach(zip(fetch!(NamedTuple, res)...)) do (oid, relnamespace, relname)
        scm = oid2schema[relnamespace]
        tbl = scm |> add_table(relname)
        oid2table[oid] = tbl
    end

    # Extract columns.
    oidnum2column = Dict{Tuple{UInt32,Int16},PGColumn}()
    res = execute(conn, """
        SELECT a.attrelid, a.attnum, a.attname, a.atttypid, a.attnotnull
        FROM pg_catalog.pg_attribute a
        WHERE a.attnum > 0 AND
              NOT a.attisdropped
        ORDER BY a.attrelid, a.attnum
    """)
    foreach(zip(fetch!(NamedTuple, res)...)) do (attrelid, attnum, attname, atttypid, attnotnull)
        attrelid in keys(oid2table) || return
        tbl = oid2table[attrelid]
        typ = oid2type[atttypid]
        col = tbl |> add_column(attname, typ, attnotnull)
        oidnum2column[(attrelid, attnum)] = col
    end

    cat
end

end
