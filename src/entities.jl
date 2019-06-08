#
# Catalog structure.
#

# Sorted collections.

struct NameOrdering <: Base.Ordering
end

Base.lt(::NameOrdering, a, b) = isless(get_name(a), get_name(b))

struct SortedVector{T} <: AbstractVector{T}
    idx::Vector{T}

    SortedVector{T}() where {T} = new(T[])
end

@inline Base.size(sv::SortedVector) = size(sv.idx)

Base.IndexStyle(::Type{<:SortedVector}) = IndexLinear()

@inline Base.getindex(sv::SortedVector, k::Number) =
    sv.idx[k]

function Base.getindex(sv::SortedVector, n::AbstractString)
    r = searchsorted(sv.idx, n, order=NameOrdering())
    !isempty(r) || throw(KeyError(n))
    sv.idx[first(r)]
end

function Base.get(sv::SortedVector, n::AbstractString, default)
    r = searchsorted(sv.idx, n, order=NameOrdering())
    !isempty(r) || return default
    sv.idx[first(r)]
end

function Base.in(n::AbstractString, sv::SortedVector)
    !isempty(searchsorted(sv.idx, n, order=NameOrdering()))
end

function Base.push!(sv::SortedVector{T}, x::T) where {T}
    r = searchsorted(sv.idx, x, order=NameOrdering())
    splice!(sv.idx, r, Ref(x))
    sv
end

function Base.delete!(sv::SortedVector, n::AbstractString)
    r = searchsorted(sv.idx, x, order=NameOrdering())
    splice!(sv.idx, r)
    sv
end

struct IndexedVector{T} <: AbstractVector{T}
    data::Vector{T}
    idx::Vector{T}

    IndexedVector{T}() where {T} = new(T[], T[])
end

@inline Base.size(iv::IndexedVector) = size(iv.data)

Base.IndexStyle(::Type{<:IndexedVector}) = IndexLinear()

@inline Base.getindex(iv::IndexedVector, k::Number) =
    iv.data[k]

function Base.getindex(iv::IndexedVector, n::AbstractString)
    r = searchsorted(iv.idx, n, order=NameOrdering())
    !isempty(r) || throw(KeyError(n))
    iv.idx[first(r)]
end

function Base.get(iv::IndexedVector, n::AbstractString, default)
    r = searchsorted(iv.idx, n, order=NameOrdering())
    !isempty(r) || return default
    iv.idx[first(r)]
end

function Base.in(n::AbstractString, iv::IndexedVector)
    !isempty(searchsorted(iv.idx, n, order=NameOrdering()))
end

function Base.push!(iv::IndexedVector{T}, x::T) where {T}
    push!(iv.data, x)
    r = searchsorted(iv.idx, x, order=NameOrdering())
    splice!(iv.idx, r, Ref(x))
    iv
end

function Base.delete!(iv::IndexedVector, n::AbstractString)
    k = findfirst(x -> get_name(x) == n, iv.data)
    k === nothing || delete!(iv.data, k)
    r = searchsorted(iv.idx, x, order=NameOrdering())
    splice!(iv.idx, r)
    iv
end

# Entity containers.

@rectypes begin

"""
Model of a foreign key constraint.

* `table`: table that owns the key;
* `name`: name of the constraint;
* `columns`: columns included in the key;
* `target_table`: table targeted by the key;
* `target_columns`: columns targeted by the key;
* `on_delete`: `ON DELETE` action;
* `on_update`: `ON UPDATE` action;
* `comment`: comment on the constraint.
"""
mutable struct PGForeignKey
    linked::Bool

    table::PGTable
    name::String
    columns::Vector{PGColumn}
    target_table::PGTable
    target_columns::Vector{PGColumn}
    on_delete::String
    on_update::String
    comment::Union{String,Nothing}

    PGForeignKey(tbl, name, cols, ttbl, tcols, on_delete, on_update) =
        new(false, tbl, name, cols, ttbl, tcols, on_update, on_delete, nothing)
end

"""
Model of a unique key constraint.

* `table`: table that owns the key;
* `name`: name of the constraint;
* `columns`: columns included in the key;
* `primary`: set if this is the primary key;
* `comment`: comment on the constraint.
"""
mutable struct PGUniqueKey
    linked::Bool

    table::PGTable
    name::String
    columns::Vector{PGColumn}
    primary::Bool
    comment::Union{String,Nothing}

    PGUniqueKey(tbl, name, cols, primary) =
        new(false, tbl, name, cols, primary, nothing)
end

"""
Model of a column.

* `table`: table that owns the column;
* `name`: name of the column;
* `type`: type of the column;
* `not_null`: set if the column has `NOT NULL` constraint;
* `default`: SQL expression that calculates the default column value; or `nothing`;
* `comment`: comment on the column.
* `unique_keys`: set of unique keys that include this column;
* `foreign_keys`: set of foreign keys that include this column;
* `referring_foreign_keys`: set of foreign keys that target this column.
"""
mutable struct PGColumn
    linked::Bool

    table::PGTable
    name::String
    type::PGType
    not_null::Bool
    default::Union{String,Nothing}
    comment::Union{String,Nothing}

    unique_keys::Set{PGUniqueKey}
    foreign_keys::Set{PGForeignKey}
    referring_foreign_keys::Set{PGForeignKey}

    PGColumn(tbl, name, typ, not_null) =
        new(false, tbl, name, typ, not_null, nothing, nothing,
            Set{PGUniqueKey}(),
            Set{PGForeignKey}(),
            Set{PGForeignKey}())
end

"""
Model of a table.

* `schema`: schema that owns the table;
* `name`: name of the table;
* `comment`: comment on the table;
* `columns`: collection of table columns;
* `primary_key`: primary key of the table, if any;
* `unique_keys`: collection of unique keys defined on the table;
* `foreign_keys`: collection of foreign keys defined on the table;
* `referring_foreign_keys`: set of foreign keys that refer to this table.
"""
mutable struct PGTable
    linked::Bool

    schema::PGSchema
    name::String
    comment::Union{String,Nothing}

    columns::IndexedVector{PGColumn}
    primary_key::Union{PGUniqueKey,Nothing}
    unique_keys::SortedVector{PGUniqueKey}
    foreign_keys::SortedVector{PGForeignKey}
    referring_foreign_keys::Set{PGForeignKey}

    PGTable(scm, name) =
        new(false, scm, name, nothing,
            IndexedVector{PGColumn}(),
            nothing,
            SortedVector{PGUniqueKey}(),
            SortedVector{PGForeignKey}(),
            Set{PGForeignKey}())
end

"""
Model of a type.

* `schema`: schema that owns the type;
* `name`: name of the type;
* `labels`: vector of labels for an `ENUM` type; `nothing` otherwise;
* `comment`: comment on the type;
* `columns`: set of columns of this type.
"""
mutable struct PGType
    linked::Bool

    schema::PGSchema
    name::String
    labels::Union{Vector{String},Nothing}
    comment::Union{String,Nothing}

    columns::Set{PGColumn}

    PGType(scm, name, lbls=nothing) =
        new(false, scm, name, lbls, nothing,
            Set{PGColumn}())
end

"""
Model of a database schema.

* `catalog`: database that owns the schema;
* `name`: name of the schema;
* `comment`: comment on the schema;
* `type`: collection of types owned by the schema;
* `tables`: collection of tables owned by the schema.
"""
mutable struct PGSchema
    linked::Bool

    catalog::PGCatalog
    name::String
    comment::Union{String,Nothing}

    types::SortedVector{PGType}
    tables::SortedVector{PGTable}

    PGSchema(cat, name) =
        new(false, cat, name, nothing,
            SortedVector{PGType}(),
            SortedVector{PGTable}())
end

"""
Model of a Postgres database.

* `name`: name of the database;
* `schemas`: collection of schemas owned by the database.
"""
mutable struct PGCatalog
    name::String

    schemas::SortedVector{PGSchema}

    PGCatalog(name) =
        new(name, SortedVector{PGSchema}())
end

end

# Catalog operations.

Base.show(io::IO, cat::PGCatalog) =
    print(io, "DATABASE $(sql_name(cat.name))")

Base.getindex(cat::PGCatalog, name::AbstractString) =
    cat.schemas[name]

Base.get(cat::PGCatalog, name::AbstractString, default) =
    get(cat.schemas, name, default)

Base.in(name::AbstractString, cat::PGCatalog) =
    name in cat.schemas

Base.length(cat::PGCatalog) =
    length(cat.schemas)

Base.iterate(cat::PGCatalog, state...) =
    iterate(cat.schemas, state...)

function add_schema!(cat::PGCatalog, name::AbstractString)
    scm = PGSchema(cat, name)
    link!(scm)
end

# Some common operations.

get_name(name::AbstractString) =
    name

get_name(ety::Union{PGSchema,PGType,PGTable,PGColumn,PGUniqueKey,PGForeignKey}) =
    ety.name

function set_name!(ety::Union{PGSchema,PGType,PGTable,PGColumn,PGUniqueKey,PGForeignKey}, name::AbstractString)
    @assert ety.linked
    unlink!(ety)
    ety.name = name
    link!(ety)
end

function set_comment!(ety::Union{PGSchema,PGType,PGTable,PGColumn,PGUniqueKey,PGForeignKey}, comment::Union{AbstractString,Nothing})
    @assert ety.linked
    ety.comment = comment
    ety
end

# Schema operations.

Base.show(io::IO, scm::PGSchema) =
    print(io, "$(!scm.linked ? "DROPPED " : "")SCHEMA $(sql_name(scm.name))")

Base.getindex(scm::PGSchema, name::AbstractString) =
    scm.tables[name]

Base.get(scm::PGSchema, name::AbstractString, default) =
    get(scm.tables, name, default)

Base.in(name::AbstractString, scm::PGSchema) =
    name in scm.tables

Base.length(scm::PGSchema) =
    length(scm.tables)

Base.iterate(scm::PGSchema, state...) =
    iterate(values(scm.tables), state...)

function link!(scm::PGSchema)
    @assert !scm.linked
    @assert !(scm.name in scm.catalog.schemas)
    push!(scm.catalog.schemas, scm)
    scm.linked = true
    scm
end

function unlink!(scm::PGSchema)
    @assert scm.linked
    delete!(scm.catalog.schemas, scm.name)
    scm.linked = false
    scm
end

function remove!(scm::PGSchema)
    @assert scm.linked
    foreach(remove!, scm.tables)
    foreach(remove!, scm.types)
    unlink!(scm)
    scm.catalog
end

get_fullname(scm::PGSchema) =
    (scm.name,)

function add_type!(scm::PGSchema, name::AbstractString, lbls::Union{AbstractVector{<:AbstractString},Nothing}=nothing)
    @assert scm.linked
    typ = PGType(scm, name, lbls)
    link!(typ)
end

function add_table!(scm::PGSchema, name::AbstractString)
    @assert scm.linked
    tbl = PGTable(scm, name)
    link!(tbl)
end

# Type operations.

Base.show(io::IO, typ::PGType) =
    print(io, "$(!typ.linked ? "DROPPED " : "")TYPE $(sql_name(get_fullname(typ)))")

function link!(typ::PGType)
    @assert !typ.linked
    @assert !(typ.name in typ.schema.types)
    push!(typ.schema.types, typ)
    typ.linked = true
    typ
end

function unlink!(typ::PGType)
    @assert typ.linked
    delete!(typ.schema.types, typ.name)
    typ.linked = false
    typ
end

function remove!(typ::PGType)
    @assert typ.linked
    foreach(remove!, typ.columns)
    unlink!(typ)
    typ.schema
end

get_fullname(typ::PGType) =
    (get_fullname(typ.schema)..., typ.name)

# Table operations.

Base.show(io::IO, tbl::PGTable) =
    print(io, "$(!tbl.linked ? "DROPPED " : "")TABLE $(sql_name(get_fullname(tbl)))")

Base.getindex(tbl::PGTable, name::AbstractString) =
    tbl.columns[name]

Base.get(tbl::PGTable, name::AbstractString, default) =
    get(tbl.columns, name, default)

Base.in(name::AbstractString, tbl::PGTable) =
    name in tbl.columns

Base.length(tbl::PGTable) =
    length(tbl.columns)

Base.iterate(tbl::PGTable, state...) =
    iterate(tbl.columns, state...)

function link!(tbl::PGTable)
    @assert !tbl.linked
    @assert !(tbl.name in tbl.schema.tables)
    push!(tbl.schema.tables, tbl)
    tbl.linked = true
    tbl
end

function unlink!(tbl::PGTable)
    @assert tbl.linked
    delete!(tbl.schema.tables, tbl.name)
    tbl.linked = false
    tbl
end

function remove!(tbl::PGTable)
    @assert tbl.linked
    foreach(remove!, tbl.referring_foreign_keys)
    foreach(remove!, tbl.foreign_keys)
    foreach(remove!, tbl.unique_keys)
    foreach(remove!, tbl.columns)
    unlink!(tbl)
    tbl.schema
end

get_fullname(tbl::PGTable) =
    (get_fullname(tbl.schema)..., tbl.name)

function add_column!(tbl::PGTable, name::AbstractString, typ::PGType, not_null::Bool)
    @assert tbl.linked
    @assert typ.linked
    @assert tbl.schema.catalog === typ.schema.catalog
    col = PGColumn(tbl, name, typ, not_null)
    link!(col)
end

function add_unique_key!(tbl::PGTable, name::AbstractString, cols::Vector{PGColumn}, primary::Bool=false)
    @assert tbl.linked
    for col in cols
        @assert col.linked
        @assert col.table === tbl
    end
    uk = PGUniqueKey(tbl, name, cols, primary)
    link!(uk)
end

function add_foreign_key!(tbl::PGTable, name::AbstractString, cols::Vector{PGColumn}, ttbl::PGTable, tcols::Vector{PGColumn},
                          on_delete::String="NO ACTION", on_update::String="NO ACTION")
    @assert tbl.linked
    @assert length(cols) > 0
    for col in cols
        @assert col.linked
        @assert col.table === tbl
    end
    @assert ttbl.linked
    @assert tbl.schema.catalog === ttbl.schema.catalog
    @assert length(tcols) == length(cols)
    for tcol in tcols
        @assert tcol.linked
        @assert tcol.table === ttbl
    end
    fk = PGForeignKey(tbl, name, cols, ttbl, tcols, on_delete, on_update)
    link!(fk)
end

# Column operations.

Base.show(io::IO, col::PGColumn) =
    print(io, "$(!col.linked ? "DROPPED " : "")COLUMN $(sql_name(get_fullname(col))) $(sql_name(get_fullname(col.type))) $(col.not_null ? "NOT " : "")NULL")

function link!(col::PGColumn)
    @assert !col.linked
    @assert !(col.name in col.table.columns)
    push!(col.table.columns, col)
    push!(col.type.columns, col)
    col.linked = true
    col
end

function unlink!(col::PGColumn)
    @assert col.linked
    delete!(col.type.columns, col)
    delete!(col.table.columns, col.name)
    col.linked = false
    col
end

function remove!(col::PGColumn)
    @assert col.linked
    foreach(remove!, col.referring_foreign_keys)
    foreach(remove!, col.foreign_keys)
    foreach(remove!, col.unique_keys)
    unlink!(col)
    col.table
end

get_fullname(col::PGColumn) =
    (get_fullname(col.table)..., col.name)

function set_type!(col::PGColumn, typ::PGType)
    @assert col.linked
    @assert typ.linked
    @assert col.schema.catalog === typ.schema.catalog
    unlink!(col)
    col.type = typ
    link!(col)
end

function set_not_null!(col::PGColumn, not_null::Bool)
    @assert col.linked
    col.not_null = not_null
    col
end

function set_default!(col::PGColumn, default::Union{AbstractString,Nothing})
    @assert col.linked
    col.default = default
    col
end

# Operations on unique key constraints.

Base.show(io::IO, uk::PGUniqueKey) =
    print(io, "$(!uk.linked ? "DROPPED " : "")CONSTRAINT $(sql_name(get_fullname(uk))) $(uk.primary ? "PRIMARY KEY" : "UNIQUE") ($(sql_name(get_name.(uk.columns))))")

function link!(uk::PGUniqueKey)
    @assert !uk.linked
    @assert !(uk.name in uk.table.unique_keys)
    push!(uk.table.unique_keys, uk)
    if uk.primary
        @assert uk.table.primary_key === nothing
        uk.table.primary_key = uk
    end
    for col in uk.columns
        push!(col.unique_keys, uk)
    end
    uk.linked = true
    uk
end

function unlink!(uk::PGUniqueKey)
    @assert uk.linked
    for col in uk.columns
        delete!(col.unique_keys, uk)
    end
    if uk.primary
        uk.table.primary_key = nothing
    end
    delete!(uk.table.unique_keys, uk.name)
    uk.linked = false
    uk
end

function remove!(uk::PGUniqueKey)
    @assert uk.linked
    unlink!(uk)
    uk.table
end

get_fullname(uk::PGUniqueKey) =
    (get_fullname(uk.table)..., uk.name)

# Operations on foreign key constraints.

Base.show(io::IO, fk::PGForeignKey) =
    print(io, "$(!fk.linked ? "DROPPED " : "")CONSTRAINT $(sql_name(get_fullname(fk))) FOREIGN KEY ($(sql_name(get_name.(fk.columns)))) REFERENCES $(sql_name(get_fullname(fk.target_table))) ($(sql_name(get_name.(fk.target_columns))))")

function link!(fk::PGForeignKey)
    @assert !fk.linked
    @assert !(fk.name in fk.table.foreign_keys)
    push!(fk.table.foreign_keys, fk)
    for col in fk.columns
        push!(col.foreign_keys, fk)
    end
    push!(fk.target_table.referring_foreign_keys, fk)
    for tcol in fk.target_columns
        push!(tcol.referring_foreign_keys, fk)
    end
    fk.linked = true
    fk
end

function unlink!(fk::PGForeignKey)
    @assert fk.linked
    for tcol in fk.target_columns
        delete!(tcol.referring_foreign_keys, fk)
    end
    delete!(fk.target_table.referring_foreign_keys, fk)
    for col in fk.columns
        delete!(col.foreign_keys, fk)
    end
    delete!(fk.table.foreign_keys, fk.name)
    fk.linked = false
    fk
end

function remove!(fk::PGForeignKey)
    @assert fk.linked
    unlink!(fk)
    fk.table
end

get_fullname(fk::PGForeignKey) =
    (get_fullname(fk.table)..., fk.name)

