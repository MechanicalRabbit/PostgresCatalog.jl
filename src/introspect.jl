#
# Introspecting the database structure.
#

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
        scm = add_schema!(cat, nspname)
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
                add_type!(scm, typname, oid2labels[oid])
            else
                add_type!(scm, typname)
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
        tbl = add_table!(scm, relname)
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
        col = add_column!(tbl, attname, typ, attnotnull)
        oidnum2column[(attrelid, attnum)] = col
    end

    cat
end

