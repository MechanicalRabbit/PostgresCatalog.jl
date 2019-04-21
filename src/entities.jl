#
# Catalog structure.
#

# Entity containers.

@rectypes begin

mutable struct PGCatalog
    scm_map::Dict{String,PGSchema}

    PGCatalog() =
        new(Dict{String,PGSchema}())
end

mutable struct PGSchema
    linked::Bool

    cat::PGCatalog
    name::String

    typ_map::Dict{String,PGType}
    proc_map::Dict{Tuple{String,Vector{PGType}},PGProcedure}
    tbl_map::Dict{String,PGTable}
    seq_map::Dict{String,PGSequence}

    PGSchema(cat, name) =
        new(false, cat, name,
            Dict{String,PGType}(),
            Dict{Tuple{String,Vector{PGType}},PGProcedure}(),
            Dict{String,PGTable}(),
            Dict{String,PGSequence}())
end

mutable struct PGType
    linked::Bool

    scm::PGSchema
    name::String
    lbls::Union{Vector{String},Nothing}

    dep_set::Set{Union{PGColumn,PGProcedure}}

    PGType(scm, name, lbls=nothing) =
        new(false, scm, name, lbls, Set{Union{PGColumn,PGProcedure}}())
end

mutable struct PGProcedure
    linked::Bool

    scm::PGSchema
    name::String
    typs::Vector{PGType}
    ret_typ::PGType
    src::String

    PGProcedure(scm, name, typs, ret_typ, src) =
        new(false, scm, name, typs, ret_typ, src)
end

mutable struct PGTable
    linked::Bool

    scm::PGSchema
    name::String

    col_map::Dict{String,PGColumn}
    col_seq::Vector{PGColumn}

    PGTable(scm, name) =
        new(false, scm, name, Dict{String,PGColumn}(), PGColumn[])
end

mutable struct PGColumn
    linked::Bool

    tbl::PGTable
    name::String
    typ::PGType
    not_null::Bool
    default::Union{String,Nothing}

    PGColumn(tbl, name, typ, not_null) =
        new(false, tbl, name, typ, not_null, nothing)
end

mutable struct PGSequence
    linked::Bool

    scm::PGSchema
    name::String

    PGSequence(scm, name) =
        new(false, scm, name)
end

end

# Catalog operations.

Base.show(io::IO, cat::PGCatalog) =
    print(io, "<DATABASE>")

Base.getindex(cat::PGCatalog, name::AbstractString) =
    get_schema(cat, name)

Base.iterate(cat::PGCatalog, state...) =
    iterate(list_schemas(cat), state...)

get_schema(cat::PGCatalog, name::AbstractString) =
    cat.scm_map[name]

get_schema(cat::PGCatalog, name::AbstractString, default) =
    get(cat.scm_map, name, default)

list_schemas(cat::PGCatalog) =
    values(cat.scm_map)

function add_schema!(cat::PGCatalog, name::AbstractString)
    @assert !(name in keys(cat.scm_map))
    scm = PGSchema(cat, name)
    link!(scm)
    scm
end

# Schema operations.

Base.show(io::IO, scm::PGSchema) =
    print(io, "<$(!scm.linked ? "DROPPED " : "")SCHEMA $(sql_name(scm.name))>")

Base.getindex(scm::PGSchema, name::AbstractString) =
    get_table(scm, name)

Base.iterate(scm::PGSchema, state...) =
    iterate(list_tables(scm), state...)

function link!(scm::PGSchema)
    @assert !scm.linked
    scm.cat.scm_map[scm.name] = scm
    scm.linked = true
    scm
end

function unlink!(scm::PGSchema)
    @assert scm.linked
    delete!(scm.cat.scm_map, scm.name)
    scm.linked = false
    scm
end

function remove!(scm::PGSchema)
    @assert scm.linked
    for tbl in list_tables(scm)
        remove!(tbl)
    end
    for typ in list_types(scm)
        remove!(typ)
    end
    unlink!(scm)
    scm.cat
end

get_catalog(scm::PGSchema) =
    scm.cat

get_name(scm::PGSchema) =
    scm.name

function set_name!(scm::PGSchema, name::AbstractString)
    @assert scm.linked
    name != scm.name || return scm
    @assert !(name in keys(scm.cat.scm_map))
    unlink!(scm)
    scm.name = name
    link!(scm)
    scm
end

get_fullname(scm::PGSchema) =
    (get_name(scm),)

get_type(scm::PGSchema, name::AbstractString) =
    scm.typ_map[name]

get_type(scm::PGSchema, name::AbstractString, default) =
    get(scm.typ_map, name, default)

list_types(scm::PGSchema) =
    values(scm.typ_map)

function add_type!(scm::PGSchema, name::AbstractString, lbls::Union{AbstractVector{<:AbstractString},Nothing}=nothing)
    @assert scm.linked
    @assert !(name in keys(scm.typ_map))
    typ = PGType(scm, name, lbls)
    link!(typ)
    typ
end

list_procedures(scm::PGSchema) =
    values(scm.proc_map)

function add_procedure!(scm::PGSchema, name::AbstractString, typs::Vector{PGType}, ret_typ::PGType, src::AbstractString)
    @assert scm.linked
    @assert !((name, typs) in keys(scm.proc_map))
    proc = PGProcedure(scm, name, typs, ret_typ, src)
    link!(proc)
    proc
end

get_table(scm::PGSchema, name::AbstractString) =
    scm.tbl_map[name]

get_table(scm::PGSchema, name::AbstractString, default) =
    get(scm.tbl_map, name, default)

list_tables(scm::PGSchema) =
    values(scm.tbl_map)

function add_table!(scm::PGSchema, name::AbstractString)
    @assert scm.linked
    @assert !(name in keys(scm.tbl_map))
    tbl = PGTable(scm, name)
    link!(tbl)
    tbl
end

get_sequence(scm::PGSchema, name::AbstractString) =
    scm.seq_map[name]

get_sequence(scm::PGSchema, name::AbstractString, default) =
    get(scm.seq_map, name, default)

list_sequences(scm::PGSchema) =
    values(scm.seq_map)

function add_sequence!(scm::PGSchema, name::AbstractString)
    @assert scm.linked
    @assert !(name in keys(scm.seq_map))
    seq = PGSequence(scm, name)
    link!(seq)
    seq
end

# Type operations.

Base.show(io::IO, typ::PGType) =
    print(io, "<$(!typ.linked ? "DROPPED " : "")TYPE $(sql_name(get_fullname(typ)))>")

function link!(typ::PGType)
    @assert !typ.linked
    typ.scm.typ_map[typ.name] = typ
    typ.linked = true
    typ
end

function unlink!(typ::PGType)
    @assert typ.linked
    delete!(typ.scm.typ_map, typ.name)
    typ.linked = false
    typ
end

function remove!(typ::PGType)
    @assert typ.linked
    for col in list_columns(typ)
        remove!(col)
    end
    unlink!(typ)
    typ.scm
end

get_schema(typ::PGType) =
    typ.scm

get_name(typ::PGType) =
    typ.name

function set_name(typ::PGType, name::AbstractString)
    @assert typ.linked
    name != typ.name || return typ
    @assert !(name in keys(typ.scm.typ_map))
    unlink!(typ)
    typ.name = name
    link!(typ)
    typ
end

get_fullname(typ::PGType) =
    (get_fullname(get_schema(typ))..., get_name(typ))

get_labels(typ::PGType) =
    typ.lbls

# Operations on stored procedures.

Base.show(io::IO, proc::PGProcedure) =
    print(io, "<$(!proc.linked ? "DROPPED " : "")PROCEDURE $(sql_name(get_fullname(proc)))($(sql_name(get_fullname.(proc.typs))))>")

function link!(proc::PGProcedure)
    @assert !proc.linked
    proc.scm.proc_map[(proc.name, proc.typs)] = proc
    for typ in proc.typs
        push!(typ.dep_set, proc)
    end
    push!(proc.ret_typ.dep_set, proc)
    proc.linked = true
    proc
end

function unlink!(proc::PGProcedure)
    @assert proc.linked
    delete!(proc.ret_typ.dep_set, proc)
    for typ in proc.typs
        delete!(typ.dep_set, proc)
    end
    delete!(proc.scm.proc_map, proc.name)
    proc.linked = false
    proc
end

function remove!(proc::PGProcedure)
    @assert proc.linked
    unlink!(proc)
    proc.scm
end

get_schema(proc::PGProcedure) =
    proc.scm

get_name(proc::PGProcedure) =
    proc.name

function set_name!(proc::PGProcedure, name::AbstractString)
    @assert proc.linked
    name != proc.name || return tbl
    @assert !(name in keys(tbl.scm.tbl_map))
    unlink!(tbl)
    tbl.name = name
    link!(tbl)
    tbl
end

get_fullname(proc::PGProcedure) =
    (get_fullname(get_schema(proc))..., get_name(proc))

get_types(proc::PGProcedure) =
    proc.typs

get_return_type(proc::PGProcedure) =
    proc.ret_typ

get_source(proc::PGProcedure) =
    proc.src

# Table operations.

Base.show(io::IO, tbl::PGTable) =
    print(io, "<$(!tbl.linked ? "DROPPED " : "")TABLE $(sql_name(get_fullname(tbl)))>")

Base.getindex(tbl::PGTable, name::AbstractString) =
    get_column(tbl, name)

Base.iterate(tbl::PGTable, state...) =
    iterate(list_columns(tbl), state...)

function link!(tbl::PGTable)
    @assert !tbl.linked
    tbl.scm.tbl_map[tbl.name] = tbl
    tbl.linked = true
    tbl
end

function unlink!(tbl::PGTable)
    @assert tbl.linked
    delete!(typ.scm.tbl_map, tbl.name)
    tbl.linked = false
    tbl
end

function remove!(tbl::PGTable)
    @assert tbl.linked
    for col in list_columns(tbl)
        remove!(col)
    end
    unlink!(tbl)
    tbl.scm
end

get_schema(tbl::PGTable) =
    tbl.scm

get_name(tbl::PGTable) =
    tbl.name

function set_name!(tbl::PGTable, name::AbstractString)
    @assert tbl.linked
    name != tbl.name || return tbl
    @assert !(name in keys(tbl.scm.tbl_map))
    unlink!(tbl)
    tbl.name = name
    link!(tbl)
    tbl
end

get_fullname(tbl::PGTable) =
    (get_fullname(get_schema(tbl))..., get_name(tbl))

get_column(tbl::PGTable, name::AbstractString) =
    tbl.col_map[name]

get_column(tbl::PGTable, name::AbstractString, default) =
    get(tbl.col_map, name, default)

list_columns(tbl::PGTable) =
    values(tbl.col_map)

function add_column!(tbl::PGTable, name::AbstractString, typ::PGType, not_null::Bool)
    @assert tbl.linked
    @assert typ.linked
    @assert tbl.scm.cat === typ.scm.cat
    @assert !(name in keys(tbl.col_map))
    col = PGColumn(tbl, name, typ, not_null)
    link!(col)
    col
end

# Column operations.

Base.show(io::IO, col::PGColumn) =
    print(io, "<$(!col.linked ? "DROPPED " : "")COLUMN $(sql_name(get_fullname(col))) $(sql_name(get_fullname(col.typ))) $(col.not_null ? "NOT " : "")NULL>")

function link!(col::PGColumn)
    @assert !col.linked
    col.tbl.col_map[col.name] = col
    push!(col.tbl.col_seq, col)
    push!(col.typ.dep_set, col)
    col.linked = true
    col
end

function unlink!(col::PGColumn)
    @assert col.linked
    delete!(col.tbl.col_map, col.name)
    filter!(!=(col), col.tbl.col_seq)
    delete!(col.typ.dep_set, col)
    col.linked = false
    col
end

function remove!(col::PGColumn)
    @assert col.linked
    unlink!(col)
    col.tbl
end

get_table(col::PGColumn) =
    col.tbl

get_name(col::PGColumn) =
    col.name

function set_name!(col::PGColumn, name::AbstractString)
    @assert col.linked
    name != col.name || return col
    @assert !(name in keys(col.tbl.col_map))
    unlink!(col)
    col.name = name
    link!(col)
    col
end

get_fullname(col::PGColumn) =
    (get_fullname(get_table(col))..., get_name(col))

get_type(col::PGColumn) =
    col.typ

get_not_null(col::PGColumn) =
    col.not_null

function set_not_null!(col::PGColumn, not_null::Bool)
    @assert col.linked
    col.not_null = not_null
    col
end

get_default(col::PGColumn) =
    col.default

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
    seq.scm.seq_map[seq.name] = seq
    seq.linked = true
    seq
end

function unlink!(seq::PGSequence)
    @assert seq.linked
    delete!(seq.scm.seq_map, seq.name)
    seq.linked = false
    seq
end

function remove!(seq::PGSequence)
    @assert seq.linked
    unlink!(seq)
    seq.scm
end

get_schema(seq::PGSequence) =
    seq.scm

get_name(seq::PGSequence) =
    seq.name

function set_name!(seq::PGSequence, name::AbstractString)
    @assert seq.linked
    name != seq.name || return tbl
    @assert !(name in keys(seq.scm.seq_map))
    unlink!(seq)
    seq.name = name
    link!(seq)
    seq
end

get_fullname(seq::PGSequence) =
    (get_fullname(get_schema(seq))..., get_name(seq))

