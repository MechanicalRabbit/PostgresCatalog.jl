#
# Catalog structure.
#

# Entity containers.

@rectypes begin

mutable struct PGTrigger
    linked::Bool

    table::PGTable
    name::String
    procedure::PGProcedure

    PGTrigger(tbl, name, proc) =
        new(false, tbl, name, proc)
end

mutable struct PGProcedure
    linked::Bool

    schema::PGSchema
    name::String
    types::Vector{PGType}
    return_type::PGType
    source::String

    triggers::Set{PGTrigger}

    PGProcedure(scm, name, typs, ret_typ, src) =
        new(false, scm, name, typs, ret_typ, src,
            Set{PGTrigger}())
end

mutable struct PGForeignKey
    linked::Bool

    table::PGTable
    name::String
    columns::Vector{PGColumn}
    target_table::PGTable
    target_columns::Vector{PGColumn}
    on_delete::Symbol
    on_update::Symbol

    PGForeignKey(tbl, name, cols, ttbl, tcols, on_delete, on_update) =
        new(false, tbl, name, cols, ttbl, tcols, on_update, on_delete)
end

mutable struct PGUniqueKey
    linked::Bool

    table::PGTable
    name::String
    columns::Vector{PGColumn}
    primary::Bool

    PGUniqueKey(tbl, name, cols, primary) =
        new(false, tbl, name, cols, primary)
end

mutable struct PGIndex
    linked::Bool

    schema::PGSchema
    name::String
    table::PGTable
    columns::Vector{PGColumn}

    PGIndex(scm, name, tbl, cols) =
        new(false, scm, name, tbl, cols)
end

mutable struct PGSequence
    linked::Bool

    schema::PGSchema
    name::String
    column::Union{PGColumn,Nothing}

    PGSequence(scm, name) =
        new(false, scm, name, nothing)
end

mutable struct PGColumn
    linked::Bool

    table::PGTable
    name::String
    type_::PGType
    not_null::Bool
    default::Union{String,Nothing}

    sequences::Set{PGSequence}
    indexes::Set{PGIndex}
    unique_keys::Set{PGUniqueKey}
    foreign_keys::Set{PGForeignKey}
    referring_foreign_keys::Set{PGForeignKey}

    PGColumn(tbl, name, typ, not_null) =
        new(false, tbl, name, typ, not_null, nothing,
            Set{PGSequence}(),
            Set{PGIndex}(),
            Set{PGUniqueKey}(),
            Set{PGForeignKey}(),
            Set{PGForeignKey}())
end

mutable struct PGTable
    linked::Bool

    schema::PGSchema
    name::String

    columns::Dict{String,PGColumn}
    indexes::Set{PGIndex}
    primary_key::Union{PGUniqueKey,Nothing}
    unique_keys::Dict{String,PGUniqueKey}
    foreign_keys::Dict{String,PGForeignKey}
    referring_foreign_keys::Set{PGForeignKey}
    triggers::Dict{String,PGTrigger}

    PGTable(scm, name) =
        new(false, scm, name,
            Dict{String,PGColumn}(),
            Set{PGIndex}(),
            nothing,
            Dict{String,PGUniqueKey}(),
            Dict{String,PGForeignKey}(),
            Set{PGForeignKey}(),
            Dict{String,PGTrigger}())
end

mutable struct PGType
    linked::Bool

    schema::PGSchema
    name::String
    labels::Union{Vector{String},Nothing}

    columns::Set{PGColumn}
    procedures::Set{PGProcedure}

    PGType(scm, name, lbls=nothing) =
        new(false, scm, name, lbls,
            Set{PGColumn}(),
            Set{PGProcedure}())
end

mutable struct PGSchema
    linked::Bool

    catalog::PGCatalog
    name::String

    types::Dict{String,PGType}
    tables::Dict{String,PGTable}
    indexes::Dict{String,PGIndex}
    sequences::Dict{String,PGSequence}
    procedures::Dict{Tuple{String,Vector{PGType}},PGProcedure}

    PGSchema(cat, name) =
        new(false, cat, name,
            Dict{String,PGType}(),
            Dict{String,PGTable}(),
            Dict{String,PGIndex}(),
            Dict{String,PGSequence}(),
            Dict{Tuple{String,Vector{PGType}},PGProcedure}())
end

mutable struct PGCatalog
    schemas::Dict{String,PGSchema}

    PGCatalog() =
        new(Dict{String,PGSchema}())
end

end

# Catalog operations.

Base.show(io::IO, cat::PGCatalog) =
    print(io, "<DATABASE>")

Base.getindex(cat::PGCatalog, name::AbstractString) =
    cat.schemas[name]

Base.get(cat::PGCatalog, name::AbstractString, default) =
    get(cat.schemas, name, default)

Base.iterate(cat::PGCatalog, state...) =
    iterate(values(cat.schemas), state...)

function add_schema!(cat::PGCatalog, name::AbstractString)
    scm = PGSchema(cat, name)
    link!(scm)
end

get_name(ety::Union{PGSchema,PGType,PGTable,PGColumn,PGSequence,PGIndex,PGUniqueKey,PGForeignKey,PGProcedure,PGTrigger}) =
    ety.name

# Schema operations.

Base.show(io::IO, scm::PGSchema) =
    print(io, "<$(!scm.linked ? "DROPPED " : "")SCHEMA $(sql_name(scm.name))>")

Base.getindex(scm::PGSchema, name::AbstractString) =
    scm.tables[name]

Base.get(scm::PGSchema, name::AbstractString, default) =
    get(scm.tables, name, default)

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
    foreach(remove!, collect(values(scm.procedures)))
    foreach(remove!, collect(values(scm.sequences)))
    foreach(remove!, collect(values(scm.indexes)))
    foreach(remove!, collect(values(scm.tables)))
    foreach(remove!, collect(values(scm.types)))
    unlink!(scm)
    scm.catalog
end

get_fullname(scm::PGSchema) =
    (scm.name,)

function set_name!(scm::PGSchema, name::AbstractString)
    @assert scm.linked
    unlink!(scm)
    scm.name = name
    link!(scm)
end

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

function add_index!(name::AbstractString, tbl::PGTable, cols::Vector{PGColumn})
    @assert tbl.linked
    @assert length(cols) > 0
    for col in cols
        @assert cols.linked
        @assert cols.table === tbl
    end
    scm = tbl.schema
    idx = PGIndex(scm, name, tbl, cols)
    link!(idx)
end

function add_sequence!(scm::PGSchema, name::AbstractString)
    @assert scm.linked
    seq = PGSequence(scm, name)
    link!(seq)
end

function add_procedure!(scm::PGSchema, name::AbstractString, typs::Vector{PGType}, ret_typ::PGType, src::AbstractString)
    @assert scm.linked
    for typ in typs
        @assert typ.linked
        @assert typ.schema.catalog === scm.catalog
    end
    @assert ret_typ.linked
    @assert ret_typ.schema.catalog === scm.catalog
    proc = PGProcedure(scm, name, typs, ret_typ, src)
    link!(proc)
end

function add_trigger!(tbl::PGTable, name::String, proc::PGProcedure)
    @assert tbl.linked
    @assert proc.linked
    @assert tbl.schema.catalog === proc.schema.catalog
    tg = PGTrigger(tbl, name, proc)
    link!(tg)
end

# Type operations.

Base.show(io::IO, typ::PGType) =
    print(io, "<$(!typ.linked ? "DROPPED " : "")TYPE $(sql_name(get_fullname(typ)))>")

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
    foreach(remove!, collect(typ.procedures))
    foreach(remove!, collect(typ.columns))
    unlink!(typ)
    typ.schema
end

get_fullname(typ::PGType) =
    (get_fullname(typ.schema)..., typ.name)

function set_name(typ::PGType, name::AbstractString)
    @assert typ.linked
    unlink!(typ)
    typ.name = name
    link!(typ)
end

# Table operations.

Base.show(io::IO, tbl::PGTable) =
    print(io, "<$(!tbl.linked ? "DROPPED " : "")TABLE $(sql_name(get_fullname(tbl)))>")

Base.getindex(tbl::PGTable, name::AbstractString) =
    tbl.columns[name]

Base.get(tbl::PGTable, name::AbstractString, default) =
    get(tbl.columns, name, default)

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
    foreach(remove!, collect(tbl.triggers))
    foreach(remove!, collect(tbl.referring_foreign_keys))
    foreach(remove!, collect(values(tbl.foreign_keys)))
    foreach(remove!, collect(values(tbl.unique_keys)))
    foreach(remove!, collect(tbl.indexes))
    foreach(remove!, collect(values(tbl.columns)))
    unlink!(tbl)
    tbl.schema
end

get_fullname(tbl::PGTable) =
    (get_fullname(tbl.schema)..., tbl.name)

function set_name!(tbl::PGTable, name::AbstractString)
    @assert tbl.linked
    unlink!(tbl)
    tbl.name = name
    link!(tbl)
end

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

function add_foreign_key!(tbl::PGTable, name::AbstractString, cols::Vector{PGColumn}, ttbl::PGTable, tcols::Vector{PGColumn}, on_delete::Symbol=:no_action, on_update::Symbol=:no_action)
    @assert tbl.linked
    for col in cols
        @assert col.linked
        @assert col.table === tbl
    end
    @assert ttbl.linked
    @assert tbl.schema.catalog === ttbl.schema.catalog
    for tcol in tcols
        @assert tcol.linked
        @assert tcol.table === ttbl
    end
    fk = PGForeignKey(tbl, name, cols, ttbl, tcols, on_delete, on_update)
    link!(fk)
end

# Column operations.

Base.show(io::IO, col::PGColumn) =
    print(io, "<$(!col.linked ? "DROPPED " : "")COLUMN $(sql_name(get_fullname(col))) $(sql_name(get_fullname(col.type_))) $(col.not_null ? "NOT " : "")NULL>")

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
    foreach(remove!, collect(col.indexes))
    unlink!(col)
    col.table
end

get_fullname(col::PGColumn) =
    (get_fullname(col.table)..., col.name)

function set_name!(col::PGColumn, name::AbstractString)
    @assert col.linked
    unlink!(col)
    col.name = name
    link!(col)
end

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

# Operations on sequences.

Base.show(io::IO, seq::PGSequence) =
    print(io, "<$(!seq.linked ? "DROPPED " : "")SEQUENCE $(sql_name(get_fullname(seq)))>")

function link!(seq::PGSequence)
    @assert !seq.linked
    @assert !(seq.name in keys(seq.schema.sequences))
    seq.schema.sequences[seq.name] = seq
    if seq.column !== nothing
        push!(seq.column.sequences, seq)
    end
    seq.linked = true
    seq
end

function unlink!(seq::PGSequence)
    @assert seq.linked
    if seql.column !== nothing
        delete!(seq.column.sequences, seq)
    end
    delete!(seq.schema.sequences, seq.name)
    seq.linked = false
    seq
end

function remove!(seq::PGSequence)
    @assert seq.linked
    unlink!(seq)
    seq.schema
end

get_fullname(seq::PGSequence) =
    (get_fullname(seq.schema)..., seq.name)

function set_name!(seq::PGSequence, name::AbstractString)
    @assert seq.linked
    unlink!(seq)
    seq.name = name
    link!(seq)
end

function set_column!(seq::PGSequence, col::Union{PGColumn,Nothing})
    @assert seq.linked
    if col !== nothing
        @assert col.linked
        @assert seq.schema.catalog === col.table.schema.catalog
    end
    unlink!(seq)
    seq.column = col
    link!(seq)
end

# Operations on indexes.

Base.show(io::IO, idx::PGIndex) =
    print(io, "<$(!idx.linked ? "DROPPED " : "")INDEX $(sql_name(get_fullname(idx)))>")

function link!(idx::PGIndex)
    @assert !idx.linked
    @assert !(idx.name in idx.schema.indexes)
    idx.schema.indexes[idx.name] = idx
    push!(idx.table.indexes, idx)
    for col in idx.columns
        push!(col.indexes, idx)
    end
    idx.linked = true
    idx
end

function unlink!(idx::PGIndex)
    @assert idx.linked
    for col in idx.columns
        delete!(col.indexes, idx)
    end
    delete!(idx.table.indexes, idx)
    delete!(idx.schema.indexes, idx.name)
    idx.linked = false
    idx
end

function remove!(idx::PGIndex)
    @assert idx.linked
    unlink!(idx)
    idx.schema
end

get_fullname(idx::PGIndex) =
    (get_fullname(idx.schema)..., idx.name)

function set_name!(idx::PGIndex, name::AbstractString)
    @assert idx.linked
    unlink!(idx)
    idx.name = name
    link!(idx)
end

# Operations on unique key constraints.

Base.show(io::IO, uk::PGUniqueKey) =
    print(io, "<$(!uk.linked ? "DROPPED " : "")$(uk.primary ? "PRIMARY" : "UNIQUE") KEY $(sql_name(get_fullname(uk))) ($(sql_name(get_name.(uk.columns))))>")

function link!(uk::PGUniqueKey)
    @assert !uk.linked
    @assert !(uk.name in uk.table.unique_keys)
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

function set_name!(uk::PGUniqueKey, name::AbstractString)
    @assert uk.linked
    unlink!(uk)
    uk.name = name
    link!(uk)
end

# Operations on foreign key constraints.

Base.show(io::IO, fk::PGForeignKey) =
print(io, "<$(!fk.linked ? "DROPPED " : "")FOREIGN KEY $(sql_name(get_fullname(fk))) ($(sql_name(get_name.(fk.columns)))) REFERENCES $(sql_name(get_fullname(fk.target_table))) ($(sql_name(get_name.(fk.target_columns))))>")

function link!(fk::PGForeignKey)
    @assert !fk.linked
    @assert !(fk.name in fk.table.foreign_keys)
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

function set_name!(fk::PGForeignKey, name::AbstractString)
    @assert fk.linked
    unlink!(fk)
    fk.name = name
    link!(fk)
end

# Operations on stored procedures.

Base.show(io::IO, proc::PGProcedure) =
    print(io, "<$(!proc.linked ? "DROPPED " : "")PROCEDURE $(sql_name(get_fullname(proc)))($(sql_name(get_fullname.(proc.types))))>")

function link!(proc::PGProcedure)
    @assert !proc.linked
    key = (proc.name, proc.types)
    @assert !(key in keys(proc.schema.procedures))
    proc.schema.procedures[key] = proc
    for typ in proc.types
        push!(typ.procedures, proc)
    end
    push!(proc.return_type.procedures, proc)
    proc.linked = true
    proc
end

function unlink!(proc::PGProcedure)
    @assert proc.linked
    delete!(proc.return_type.procedures, proc)
    for typ in proc.types
        delete!(typ.procedures, proc)
    end
    delete!(proc.schema.procedures, (proc.name, proc.types))
    proc.linked = false
    proc
end

function remove!(proc::PGProcedure)
    @assert proc.linked
    unlink!(proc)
    proc.schema
end

get_fullname(proc::PGProcedure) =
    (get_fullname(proc.schema)..., proc.name)

function set_name!(proc::PGProcedure, name::AbstractString)
    @assert proc.linked
    unlink!(proc)
    proc.name = name
    link!(proc)
end

# Operations on triggers.

Base.show(io::IO, tg::PGTrigger) =
    print(io, "<$(!tg.linked ? "DROPPED " : "")TRIGGER $(sql_name(get_fullname(tg)))>")

function link!(tg::PGTrigger)
    @assert !tg.linked
    @assert !(tg.name in tg.table.triggers)
    tg.table.triggers[tg.name] = tg
    push!(tg.procedure.triggers, tg)
    tg.linked = true
    tg
end

function unlink!(tg::PGTrigger)
    @assert tg.linked
    delete!(tg.procedure.triggers, tg)
    delete!(tg.table.triggers, tg.name)
    tg.linked = false
    tg
end

function remove!(tg::PGTrigger)
    @assert tg.linked
    unlink!(tg)
    tg.table
end

get_fullname(tg::PGTrigger) =
    (get_fullname(tg.table)..., tg.name)

function set_name!(tg::PGTrigger, name::AbstractString)
    @assert tg.linked
    unlink!(tg)
    tg.name = name
    link!(tg)
end

