#
# Catalog structure.
#

# Entity containers.

@rectypes begin

mutable struct PGCatalog
    schemas::Dict{String,PGSchema}

    PGCatalog() =
        new(Dict{String,PGSchema}())
end

mutable struct PGSchema
    linked::Bool

    catalog::PGCatalog
    name::String

    types::Dict{String,PGType}
    procedures::Dict{Tuple{String,Vector{PGType}},PGProcedure}
    tables::Dict{String,PGTable}
    sequences::Dict{String,PGSequence}

    PGSchema(cat, name) =
        new(false, cat, name,
            Dict{String,PGType}(),
            Dict{Tuple{String,Vector{PGType}},PGProcedure}(),
            Dict{String,PGTable}(),
            Dict{String,PGSequence}())
end

mutable struct PGType
    linked::Bool

    schema::PGSchema
    name::String
    labels::Union{Vector{String},Nothing}

    dependencies::Set{Union{PGColumn,PGProcedure}}

    PGType(scm, name, lbls=nothing) =
        new(false, scm, name, lbls, Set{Union{PGColumn,PGProcedure}}())
end

mutable struct PGProcedure
    linked::Bool

    schema::PGSchema
    name::String
    types::Vector{PGType}
    return_type::PGType
    source::String

    PGProcedure(scm, name, typs, ret_typ, src) =
        new(false, scm, name, typs, ret_typ, src)
end

mutable struct PGTable
    linked::Bool

    schema::PGSchema
    name::String

    columns::Dict{String,PGColumn}

    PGTable(scm, name) =
        new(false, scm, name, Dict{String,PGColumn}())
end

mutable struct PGColumn
    linked::Bool

    table::PGTable
    name::String
    type_::PGType
    not_null::Bool
    default::Union{String,Nothing}

    PGColumn(tbl, name, typ, not_null) =
        new(false, tbl, name, typ, not_null, nothing)
end

mutable struct PGSequence
    linked::Bool

    schema::PGSchema
    name::String

    PGSequence(scm, name) =
        new(false, scm, name)
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
    for tbl in values(scm.tables)
        remove!(tbl)
    end
    for typ in values(scm.types)
        remove!(typ)
    end
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

function add_procedure!(scm::PGSchema, name::AbstractString, typs::Vector{PGType}, ret_typ::PGType, src::AbstractString)
    @assert scm.linked
    foreach(typs) do typ
        @assert typ.linked
        @assert typ.schema.catalog === scm.catalog
    end
    @assert ret_typ.linked
    @assert ret_typ.schema.catalog === scm.catalog
    proc = PGProcedure(scm, name, typs, ret_typ, src)
    link!(proc)
end

function add_table!(scm::PGSchema, name::AbstractString)
    @assert scm.linked
    tbl = PGTable(scm, name)
    link!(tbl)
end

function add_sequence!(scm::PGSchema, name::AbstractString)
    @assert scm.linked
    seq = PGSequence(scm, name)
    link!(seq)
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
    for dep in typ.dependencies
        remove!(dep)
    end
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

# Operations on stored procedures.

Base.show(io::IO, proc::PGProcedure) =
    print(io, "<$(!proc.linked ? "DROPPED " : "")PROCEDURE $(sql_name(get_fullname(proc)))($(sql_name(get_fullname.(proc.types))))>")

function link!(proc::PGProcedure)
    @assert !proc.linked
    key = (proc.name, proc.types)
    @assert !(key in keys(proc.schema.procedures))
    proc.schema.procedures[key] = proc
    for typ in proc.types
        push!(typ.dependencies, proc)
    end
    push!(proc.return_type.dependencies, proc)
    proc.linked = true
    proc
end

function unlink!(proc::PGProcedure)
    @assert proc.linked
    delete!(proc.return_type.dependencies, proc)
    for typ in proc.types
        delete!(typ.dependencies, proc)
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

function set_name!(proc::PGProcedure, name::AbstractString)
    @assert proc.linked
    unlink!(proc)
    proc.name = name
    link!(proc)
end

get_fullname(proc::PGProcedure) =
    (get_fullname(proc.schema)..., proc.name)

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
    for col in values(tbl.columns)
        remove!(col)
    end
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
    @assert tbl.schema.catalog == typ.schema.catalog
    col = PGColumn(tbl, name, typ, not_null)
    link!(col)
end

# Column operations.

Base.show(io::IO, col::PGColumn) =
    print(io, "<$(!col.linked ? "DROPPED " : "")COLUMN $(sql_name(get_fullname(col))) $(sql_name(get_fullname(col.type_))) $(col.not_null ? "NOT " : "")NULL>")

function link!(col::PGColumn)
    @assert !col.linked
    @assert !(col.name in keys(col.table.columns))
    col.table.columns[col.name] = col
    push!(col.type_.dependencies, col)
    col.linked = true
    col
end

function unlink!(col::PGColumn)
    @assert col.linked
    delete!(col.type_.dependencies, col)
    delete!(col.table.columns, col.name)
    col.linked = false
    col
end

function remove!(col::PGColumn)
    @assert col.linked
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
    seq.linked = true
    seq
end

function unlink!(seq::PGSequence)
    @assert seq.linked
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
    seq
end

