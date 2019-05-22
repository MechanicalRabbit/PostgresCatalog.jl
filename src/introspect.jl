#
# Introspecting the database structure.
#

"""
    introspect(conn) :: PGCatalog

Introspects the structure of a Postgres database.
"""
function introspect(conn)
    cat = PGCatalog()

    # Extract schemas.
    oid2schema = Dict{UInt32,PGSchema}()
    res = execute(conn, """
        SELECT n.oid, n.nspname
        FROM pg_catalog.pg_namespace n
        ORDER BY n.nspname
    """)
    foreach(columntable(res)...) do oid, nspname
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
    foreach(columntable(res)...) do enumtypid, enumlabel
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
    foreach(columntable(res)...) do oid, typnamespace, typname, typtype
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
    foreach(columntable(res)...) do oid, relnamespace, relname
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
    foreach(columntable(res)...) do attrelid, attnum, attname, atttypid, attnotnull
        attrelid in keys(oid2table) || return
        tbl = oid2table[attrelid]
        typ = oid2type[atttypid]
        col = add_column!(tbl, attname, typ, attnotnull)
        oidnum2column[(attrelid, attnum)] = col
    end

    # Extract default values.
    res = execute(conn, """
        SELECT a.adrelid, a.adnum, pg_get_expr(a.adbin, a.adrelid)
        FROM pg_catalog.pg_attrdef a
        ORDER BY a.adrelid, a.adnum
    """)
    foreach(columntable(res)...) do adrelid, adnum, adsrc
        (adrelid, adnum) in keys(oidnum2column) || return
        col = oidnum2column[(adrelid, adnum)]
        set_default!(col, adsrc)
    end

    # Extract sequences.
    oid2sequence = Dict{UInt32,PGSequence}()
    res = execute(conn, """
        SELECT c.oid, c.relnamespace, c.relname
        FROM pg_catalog.pg_class c
        WHERE c.relkind = 'S'
        ORDER BY c.relnamespace, c.relname
    """)
    foreach(columntable(res)...) do oid, relnamespace, relname
        scm = oid2schema[relnamespace]
        seq = add_sequence!(scm, relname)
        oid2sequence[oid] = seq
    end

    # Extract sequence owners.
    res = execute(conn, """
        SELECT d.objid, d.refobjid, d.refobjsubid
        FROM pg_catalog.pg_depend d JOIN
             pg_catalog.pg_class c ON (d.classid = 'pg_class'::regclass AND d.objid = c.oid)
        WHERE c.relkind = 'S' AND
              d.refclassid = 'pg_class'::regclass AND
              d.objsubid IS NOT NULL
        ORDER BY d.objid, d.refobjid, d.objsubid
    """)
    foreach(columntable(res)...) do objid, refobjid, refobjsubid
        (refobjid, refobjsubid) in keys(oidnum2column) || return
        seq = oid2sequence[objid]
        col = oidnum2column[(refobjid, refobjsubid)]
        set_column!(seq, col)
    end

    # Extract indexes.
    res = execute(conn, """
        SELECT c.oid, c.relnamespace, c.relname, i.indrelid, i.indkey
        FROM pg_catalog.pg_class c JOIN
             pg_catalog.pg_index i ON (c.oid = i.indexrelid)
        WHERE c.relkind = 'i'
        ORDER BY c.relnamespace, c.relname
    """)
    foreach(columntable(res)...) do oid, relnamespace, relname, indrelid, indkey
        indrelid in keys(oid2table) || return
        scm = oid2schema[relnamespace]
        tbl = oid2table[indrelid]
        cols = PGColumn[]
        for num in parse.(Int16, split(indkey))
            num > 0 || return
            push!(cols, oidnum2column[(indrelid, num)])
        end
        add_index!(scm, relname, tbl, cols)
    end

    # Extract unique keys.
    oid2unique_key = Dict{UInt32,PGUniqueKey}()
    res = execute(conn, """
        SELECT c.oid, c.conname, c.contype, c.conrelid, c.conkey
        FROM pg_catalog.pg_constraint c
        WHERE c.contype IN ('p', 'u')
        ORDER BY c.oid
    """)
    foreach(columntable(res)...) do oid, conname, contype, conrelid, conkey
        conrelid in keys(oid2table) || return
        tbl = oid2table[conrelid]
        cols = PGColumn[]
        for num in conkey
            num > 0 || return
            push!(cols, oidnum2column[(conrelid, num)])
        end
        primary = contype == 'p'
        uk = add_unique_key!(tbl, conname, cols, primary)
        oid2unique_key[oid] = uk
    end

    # Extract foreign keys.
    oid2foreign_key = Dict{UInt32,PGForeignKey}()
    res = execute(conn, """
        SELECT c.oid, c.conname, c.conrelid, c.conkey, c.confrelid, c.confkey,
               c.confdeltype, c.confupdtype
        FROM pg_catalog.pg_constraint c
        WHERE c.contype = 'f'
        ORDER BY c.oid
    """)
    foreach(columntable(res)...) do oid, conname, conrelid, conkey, confrelid, confkey, confdeltype, confupdtype
        conrelid in keys(oid2table) || return
        tbl = oid2table[conrelid]
        cols = PGColumn[]
        for num in conkey
            num > 0 || return
            push!(cols, oidnum2column[(conrelid, num)])
        end
        confrelid in keys(oid2table) || return
        ttbl = oid2table[confrelid]
        tcols = PGColumn[]
        for num in confkey
            num > 0 || return
            push!(tcols, oidnum2column[(confrelid, num)])
        end
        on_delete = confdeltype == 'a' ? "NO ACTION" :
                    confdeltype == 'r' ? "RESTRICT" :
                    confdeltype == 'c' ? "CASCADE" :
                    confdeltype == 'n' ? "SET NULL" :
                    confdeltype == 'd' ? "SET DEFAULT" : ""
        on_update = confupdtype == 'a' ? "NO ACTION" :
                    confupdtype == 'r' ? "RESTRICT" :
                    confupdtype == 'c' ? "CASCADE" :
                    confupdtype == 'n' ? "SET NULL" :
                    confupdtype == 'd' ? "SET DEFAULT" : ""
        fk = add_foreign_key!(tbl, conname, cols, ttbl, tcols, on_delete, on_update)
        oid2foreign_key[oid] = fk
    end

    # Extract stored procedures.
    oid2procedure = Dict{UInt32,PGProcedure}()
    res = execute(conn, """
        SELECT p.oid, p.pronamespace, p.proname, p.proargtypes, p.prorettype, p.prosrc
        FROM pg_catalog.pg_proc p
        ORDER BY p.pronamespace, p.proname
    """)
    foreach(columntable(res)...) do oid, pronamespace, proname, proargtypes, prorettype, prosrc
        scm = oid2schema[pronamespace]
        typs = getindex.(Ref(oid2type), parse.(UInt32, split(proargtypes)))
        ret_typ = oid2type[prorettype]
        proc = add_procedure!(scm, proname, typs, ret_typ, prosrc)
        oid2procedure[oid] = proc
    end

    # Extract triggers.
    oid2trigger = Dict{UInt32,PGTrigger}()
    res = execute(conn, """
        SELECT t.oid, t.tgrelid, t.tgname, t.tgfoid
        FROM pg_catalog.pg_trigger t
        WHERE NOT t.tgisinternal
        ORDER BY t.tgrelid, t.tgname
    """)
    foreach(columntable(res)...) do oid, tgrelid, tgname, tgfoid
        tgrelid in keys(oid2table) || return
        tbl = oid2table[tgrelid]
        proc = oid2procedure[tgfoid]
        tg = add_trigger!(tbl, tgname, proc)
        oid2trigger[oid] = tg
    end

    # Extract comments
    res = execute(conn, """
        SELECT c.relname, d.objoid, d.objsubid, d.description
        FROM pg_catalog.pg_description d JOIN
             pg_catalog.pg_class c ON (d.classoid = c.oid)
        WHERE d.classoid IN ('pg_catalog.pg_namespace'::regclass,
                             'pg_catalog.pg_type'::regclass,
                             'pg_catalog.pg_class'::regclass,
                             'pg_catalog.pg_constraint'::regclass,
                             'pg_catalog.pg_proc'::regclass,
                             'pg_catalog.pg_trigger'::regclass)
        ORDER BY d.objoid, d.classoid, d.objsubid
    """)
    foreach(columntable(res)...) do relname, objoid, objsubid, description
        if relname == "pg_namespace"
            scm = oid2schema[objoid]
            set_comment!(scm, description)
        elseif relname == "pg_type"
            typ = oid2type[objoid]
            set_comment!(typ, description)
        elseif relname == "pg_class" && (objoid, objsubid) in keys(oidnum2column)
            col = oidnum2column[(objoid, objsubid)]
            set_comment!(col, description)
        elseif relname == "pg_class" && objsubid == 0 && objoid in keys(oid2table)
            tbl = oid2table[objoid]
            set_comment!(tbl, description)
        elseif relname == "pg_class" && objsubid == 0 && objoid in keys(oid2sequence)
            seq = oid2sequence[objoid]
            set_comment!(seq, description)
        elseif relname == "pg_class" && objsubid == 0 && objoid in keys(oid2index)
            idx = oid2index[objoid]
            set_comment!(idx, description)
        elseif relname == "pg_constraint" && objoid in keys(oid2unique_key)
            uk = oid2unique_key[objoid]
            set_comment!(uk, description)
        elseif relname == "pg_constraint" && objoid in keys(oid2foreign_key)
            fk = oid2foreign_key[objoid]
            set_comment!(fk, description)
        elseif relname == "pg_proc"
            proc = oid2procedure[objoid]
            set_comment!(proc, description)
        elseif relname == "pg_trigger" && objoid in keys(oid2trigger)
            tg = oid2trigger[objoid]
            set_comment!(tg, description)
        end
    end

    cat
end

