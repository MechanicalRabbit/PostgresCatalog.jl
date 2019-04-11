#
# Introspecting and managing PostgreSQL catalog.
#

module PostgresCatalog

using LibPQ

# Data containers.

mutable struct PGSchemaData
    name::String
    typ_map::Dict{String,UInt32}
    tbl_map::Dict{String,UInt32}

    alive::Bool

    PGSchemaData(name) =
        new(name, Dict{String,UInt32}(), Dict{String,UInt32}(), true)
end

mutable struct PGTypeData
    scm_ref::UInt32
    name::String
    lbls::Vector{String}
    col_set::Set{UInt32}

    alive::Bool

    PGTypeData(scm_ref, name, lbls=String[]) =
        new(scm_ref, name, lbls, Set{UInt32}(), true)
end

mutable struct PGTableData
    scm_ref::UInt32
    name::String
    col_seq::Vector{UInt32}
    col_map::Dict{String,UInt32}

    alive::Bool

    PGTableData(scm_ref, name) =
        new(scm_ref, name, UInt32[], Dict{String,UInt32}(), true)
end

mutable struct PGColumnData
    tbl_ref::UInt32
    name::String
    typ_ref::UInt32
    not_null::Bool

    alive::Bool

    PGColumnData(tbl_ref, name, typ_ref, not_null=true) =
        new(tbl_ref, name, typ_ref, not_null, true)
end

# The root container.

mutable struct PGCatalog
    scm_map::Dict{String,UInt32}

    next_ref::UInt32

    scm_idx::Dict{UInt32,PGSchemaData}
    typ_idx::Dict{UInt32,PGTypeData}
    tbl_idx::Dict{UInt32,PGTableData}
    col_idx::Dict{UInt32,PGColumnData}

    PGCatalog() =
        new(Dict{String,UInt32}(),
            1,
            Dict{UInt32,PGSchemaData}(),
            Dict{UInt32,PGTypeData}(),
            Dict{UInt32,PGTableData}(),
            Dict{UInt32,PGColumnData}())
end

# Entity wrapper.

struct PGEntity{T}
    cat::PGCatalog
    ref::UInt32
    data::T
end

const PGSchema = PGEntity{PGSchemaData}

const PGType = PGEntity{PGTypeData}

const PGTable = PGEntity{PGTableData}

const PGColumn = PGEntity{PGColumnData}

get_catalog(ety::PGEntity) =
    ety.cat

function get_catalog(ety1::PGEntity, ety2::PGEntity, etys::PGEntity...)
    @assert ety1.cat === ety2.cat
    get_catalog(ety2, etys...)
end

# Catalog operations.

Base.show(io::IO, cat::PGCatalog) =
    print(io, "<PGCatalog>")

_entity_index(cat::PGCatalog, ::Type{PGSchemaData}) =
    cat.scm_idx

_entity_index(cat::PGCatalog, ::Type{PGTypeData}) =
    cat.typ_idx

_entity_index(cat::PGCatalog, ::Type{PGTableData}) =
    cat.tbl_idx

_entity_index(cat::PGCatalog, ::Type{PGColumnData}) =
    cat.col_idx

_entity_index(cat::PGCatalog, ::Type{PGEntity{T}}) where {T} =
    _entity_index(cat, T)

_entity_index(cat::PGCatalog, data::T) where {T} =
    _entity_index(cat, T)

get_entity(cat::PGCatalog, T::Type, ref::UInt32) =
    PGEntity(cat, ref, _entity_index(cat, T)[ref])

function add_entity(cat::PGCatalog, data)
    ref = cat.next_ref
    cat.next_ref += 1
    _entity_index(cat, data)[ref] = data
    return PGEntity(cat, ref, data)
end

function add_schema(cat::PGCatalog, name::AbstractString)
    @assert !(name in keys(cat.scm_map))
    data = PGSchemaData(name)
    scm = add_entity(cat, data)
    cat.scm_map[data.name] = scm.ref
    return scm
end

function get_schema(cat::PGCatalog, name::AbstractString)
    @assert name in keys(cat.scm_map)
    ref = cat.scm_map[name]
    return get_entity(cat, PGSchema, ref)
end

Base.getindex(cat::PGCatalog, name::AbstractString) =
    get_schema(cat, name)

get_schema(cat::PGCatalog, name::AbstractString, default) =
    name in keys(cat.scm_map) ? get_schema(cat, name) : default

list_schemas(cat::PGCatalog) =
    (get_schema(cat, name) for name in keys(cat.scm_map))

# Schema operations.

Base.show(io::IO, scm::PGSchema) =
    scm.data.alive ?
        print(io, "<PGSchema: $(scm.data.name)>") :
        print(io, "<DEAD PGSchema>")

function set_name(scm::PGSchema, name::AbstractString)
    @assert scm.data.alive
    cat = get_catalog(scm)
    @assert !(name in keys(cat.scm_map))
    delete!(cat.scm_map, scm.data.name)
    scm.data.name = name
    cat.scm_map[scm.data.name] = scm.ref
    return scm
end

function get_name(scm::PGSchema)
    @assert scm.data.alive
    return scm.data.name
end

get_fullname(scm::PGSchema) =
    (get_name(scm),)

function add_type(scm::PGSchema, name::AbstractString, lbls::AbstractVector{<:AbstractString}=String[])
    @assert scm.data.alive
    @assert !(name in keys(scm.data.typ_map))
    cat = get_catalog(scm)
    data = PGTypeData(scm.ref, name, lbls)
    typ = add_entity(cat, data)
    scm.data.typ_map[typ.data.name] = typ.ref
    return typ
end

function get_type(scm::PGSchema, name::AbstractString)
    @assert scm.data.alive
    @assert name in keys(scm.data.typ_map)
    ref = scm.data.typ_map[name]
    cat = get_catalog(scm)
    return get_entity(cat, PGType, ref)
end

function get_type(scm::PGSchema, name::AbstractString, default)
    @assert scm.data.alive
    return name in keys(scm.data.typ_map) ? get_type(scm, name) : default
end

function list_types(scm::PGSchema)
    @assert scm.data.alive
    return (get_type(scm, name) for name in keys(scm.data.typ_map))
end

function add_table(scm::PGSchema, name::AbstractString)
    @assert scm.data.alive
    @assert !(name in keys(scm.data.tbl_map))
    cat = get_catalog(scm)
    data = PGTableData(scm.ref, name)
    tbl = add_entity(cat, data)
    scm.data.tbl_map[tbl.data.name] = tbl.ref
    return tbl
end

function get_table(scm::PGSchema, name::AbstractString)
    @assert scm.data.alive
    @assert name in keys(scm.data.tbl_map)
    ref = scm.data.tbl_map[name]
    cat = get_catalog(scm)
    return get_entity(cat, PGTable, ref)
end

Base.getindex(scm::PGSchema, name::AbstractString) =
    get_table(scm, name)

function get_table(scm::PGSchema, name::AbstractString, default)
    @assert scm.data.alive
    return name in keys(scm.data.tbl_map) ? get_table(scm, name) : default
end

function list_tables(scm::PGSchema)
    @assert scm.data.alive
    return (get_table(scm, name) for name in keys(scm.data.tbl_map))
end

# Table operations.

Base.show(io::IO, tbl::PGTable) =
    tbl.data.alive ?
        print(io, "<PGTable: $(join(get_fullname(tbl), '.'))>") :
        print(io, "<DEAD PGTable>")

function get_schema(tbl::PGTable)
    @assert tbl.data.alive
    cat = get_catalog(tbl)
    return get_entity(cat, PGSchema, tbl.data.scm_ref)
end

function set_name(tbl::PGTable, name::AbstractString)
    @assert tbl.data.alive
    cat = get_catalog(tbl)
    scm = get_entity(cat, PGSchema, tbl.data.scm_ref)
    @assert !(name in keys(scm.tbl_map))
    delete!(scm.tbl_map, tbl.data.name)
    tbl.data.name = name
    scm.tbl_map[tbl.data.name] = tbl.ref
    return tbl
end

function get_name(tbl::PGTable)
    @assert tbl.data.alive
    return tbl.data.name
end

get_fullname(tbl::PGTable) =
    (get_fullname(get_schema(tbl))..., get_name(tbl))

function add_column(tbl::PGTable, name::AbstractString, typ::PGType, not_null::Bool)
    @assert tbl.data.alive && typ.data.alive
    @assert !(name in keys(tbl.data.col_map))
    cat = get_catalog(tbl, typ)
    data = PGColumnData(tbl.ref, name, typ.ref, not_null)
    col = add_entity(cat, data)
    tbl.data.col_map[col.data.name] = col.ref
    push!(tbl.data.col_seq, col.ref)
    push!(typ.data.col_set, col.ref)
    return col
end

function get_column(tbl::PGTable, name::AbstractString)
    @assert tbl.data.alive
    @assert name in keys(tbl.data.col_map)
    ref = tbl.data.col_map[name]
    cat = get_catalog(tbl)
    return get_entity(cat, PGColumn, ref)
end

Base.getindex(tbl::PGTable, name::AbstractString) =
    get_column(tbl, name)

function get_column(tbl::PGTable, name::AbstractString, default)
    @assert tbl.data.alive
    return name in keys(tbl.data.col_map) ? get_column(scm, name) : default
end

function list_columns(tbl::PGTable)
    @assert tbl.data.alive
    return (get_entity(tbl.cat, PGColumn, ref) for ref in tbl.data.col_seq)
end

# Column operations.

Base.show(io::IO, col::PGColumn) =
    col.data.alive ?
        print(io, "<PGColumn: $(join(get_fullname(col), '.'))>") :
        print(io, "<DEAD PGColumn>")

function get_table(col::PGColumn)
    @assert col.data.alive
    cat = get_catalog(col)
    return get_entity(cat, PGTable, col.data.tbl_ref)
end

function get_name(col::PGColumn)
    @assert col.data.alive
    return col.data.name
end

get_fullname(col::PGColumn) =
    (get_fullname(get_table(col))..., get_name(col))

# Shortcuts.

add_schema(name) =
    cat -> add_schema(cat, name)

get_schema(name) =
    cat -> get_schema(cat, name)

get_schema(name, default) =
    cat -> get_schema(cat, name, default)

list_schemas() =
    cat -> list_schemas(cat)

add_type(name, lbls=String[]) =
    scm -> add_type(scm, name, lbls)

get_type(name) =
    scm -> get_type(scm, name)

get_type(name, default) =
    scm -> get_type(scm, name, default)

list_types() =
    scm -> list_types(scm)

add_table(name) =
    scm -> add_table(scm, name)

get_table(name) =
    scm -> get_table(scm, name)

get_table(name, default) =
    scm -> get_table(scm, name, default)

list_tables() =
    scm -> list_tables(scm)

add_column(name, typ, not_null=true) =
    tbl -> add_column(tbl, name, typ, not_null)

get_column(name) =
    tbl -> get_column(tbl, name)

get_column(name, default) =
    tbl -> get_column(tbl, name, default)

list_columns() =
    tbl -> list_columns(tbl)

# Introspection.

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
                scm |> add_type(typname, oid2labels[oid])
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
