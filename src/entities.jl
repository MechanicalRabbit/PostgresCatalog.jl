#
# Catalog structure.
#


# Entity containers.

@rectypes begin

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

mutable struct PGColumn
    linked::Bool

    table::PGTable
    name::String
    type_::PGType
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

mutable struct PGTable
    linked::Bool

    schema::PGSchema
    name::String
    comment::Union{String,Nothing}

    columns::Dict{String,PGColumn}
    primary_key::Union{PGUniqueKey,Nothing}
    unique_keys::Dict{String,PGUniqueKey}
    foreign_keys::Dict{String,PGForeignKey}
    referring_foreign_keys::Set{PGForeignKey}

    PGTable(scm, name) =
        new(false, scm, name, nothing,
            Dict{String,PGColumn}(),
            nothing,
            Dict{String,PGUniqueKey}(),
            Dict{String,PGForeignKey}(),
            Set{PGForeignKey}())
end

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

mutable struct PGSchema
    linked::Bool

    catalog::PGCatalog
    name::String
    comment::Union{String,Nothing}

    types::Dict{String,PGType}
    tables::Dict{String,PGTable}

    PGSchema(cat, name) =
        new(false, cat, name, nothing,
            Dict{String,PGType}(),
            Dict{String,PGTable}())
end

mutable struct PGCatalog
    name::String

    schemas::Dict{String,PGSchema}

    PGCatalog(name) =
        new(name, Dict{String,PGSchema}())
end

end

# Catalog operations.

Base.show(io::IO, cat::PGCatalog) =
    print(io, "DATABASE $(sql_name(cat.name))")

Base.getindex(cat::PGCatalog, name::AbstractString) =
    cat.schemas[name]

Base.get(cat::PGCatalog, name::AbstractString, default) =
    get(cat.schemas, name, default)

Base.length(cat::PGCatalog) =
    length(cat.schemas)

Base.iterate(cat::PGCatalog, state...) =
    iterate(values(cat.schemas), state...)

function add_schema!(cat::PGCatalog, name::AbstractString)
    scm = PGSchema(cat, name)
    link!(scm)
end

# Some common operations.

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

Base.length(scm::PGSchema) =
    length(scm.tables)

Base.iterate(scm::PGSchema, state...) =
    iterate(values(scm.tables), state...)

function link!(scm::PGSchema)
    @assert !scm.linked
    @assert !(scm.name in keys(scm.catalog.schemas))
    scm.catalog.schemas[scm.name] = scm
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
    foreach(remove!, collect(values(scm.tables)))
    foreach(remove!, collect(values(scm.types)))
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
    @assert !(typ.name in keys(typ.schema.types))
    typ.schema.types[typ.name] = typ
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
    foreach(remove!, collect(typ.columns))
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

Base.length(tbl::PGTable) =
    length(tbl.columns)

Base.iterate(tbl::PGTable, state...) =
    iterate(values(tbl.columns), state...)

function link!(tbl::PGTable)
    @assert !tbl.linked
    @assert !(tbl.name in keys(tbl.schema.tables))
    tbl.schema.tables[tbl.name] = tbl
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
    foreach(remove!, collect(tbl.referring_foreign_keys))
    foreach(remove!, collect(values(tbl.foreign_keys)))
    foreach(remove!, collect(values(tbl.unique_keys)))
    foreach(remove!, collect(values(tbl.columns)))
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
    print(io, "$(!col.linked ? "DROPPED " : "")COLUMN $(sql_name(get_fullname(col))) $(sql_name(get_fullname(col.type_))) $(col.not_null ? "NOT " : "")NULL")

function link!(col::PGColumn)
    @assert !col.linked
    @assert !(col.name in keys(col.table.columns))
    col.table.columns[col.name] = col
    push!(col.type_.columns, col)
    col.linked = true
    col
end

function unlink!(col::PGColumn)
    @assert col.linked
    delete!(col.type_.columns, col)
    delete!(col.table.columns, col.name)
    col.linked = false
    col
end

function remove!(col::PGColumn)
    @assert col.linked
    foreach(remove!, collect(col.referring_foreign_keys))
    foreach(remove!, collect(col.foreign_keys))
    foreach(remove!, collect(col.unique_keys))
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
    col.type_ = typ
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
    @assert !(uk.name in keys(uk.table.unique_keys))
    uk.table.unique_keys[uk.name] = uk
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
    @assert !(fk.name in keys(fk.table.foreign_keys))
    fk.table.foreign_keys[fk.name] = fk
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
