#
# Manipulations.
#

# Catalog operations.

function create_schema!(conn, cat::PGCatalog, name::AbstractString)
    scm = add_schema!(cat, name)
    sql = sql_create_schema(scm)
    @debug sql
    execute(conn, sql)
    scm
end

# Schema operations.

function alter_name!(conn, scm::PGSchema, name::AbstractString)
    name != scm.name || return scm
    old_name = scm.name
    set_name!(scm, name)
    sql = sql_rename_schema(old_name, scm)
    @debug sql
    execute(conn, sql)
    scm
end

function alter_comment!(conn, scm::PGSchema, comment::Union{AbstractString,Nothing})
    comment != scm.comment || return scm
    set_comment!(scm, comment)
    sql = sql_comment_on_schema(scm, scm.comment)
    @debug sql
    execute(conn, sql)
    scm
end

function drop!(conn, scm::PGSchema)
    cat = remove!(scm)
    sql = sql_drop_schema(scm)
    @debug sql
    execute(conn, sql)
    cat
end

function create_type!(conn, scm::PGSchema, name::AbstractString, lbls::AbstractVector{<:AbstractString})
    typ = add_type!(scm, name, lbls)
    sql = sql_create_enum_type(typ, typ.labels)
    @debug sql
    execute(conn, sql)
    typ
end

const ColumnDef = NamedTuple{(:name,:type_,:not_null,:default),<:Tuple{AbstractString,PGType,Bool,Any}}

_column_def(; name, type_, not_null, default=nothing) =
    (name = name, type_ = type_, not_null = not_null, default = default)

function create_table!(conn, scm::PGSchema, name::AbstractString, cols::Vector{ColumnDef})
    tbl = add_table!(scm, name)
    for (name, typ, not_null, default) in cols
        col = add_column!(tbl, name, typ, not_null)
        default === nothing || set_default!(col, sql_value(default))
    end
    clauses = [sql_column(col.name, col.type_, col.not_null, col.default) for col in tbl]
    sql = sql_create_table(tbl, clauses)
    @debug sql
    execute(conn, sql)
    tbl
end

create_table!(conn, scm::PGSchema, name::AbstractString, cols) =
    create_table!(conn, scm, name, ColumnDef[_column_def(; col...) for col in cols])

function create_sequence!(conn, scm::PGSchema, name::AbstractString, col::Union{PGColumn,Nothing}=nothing)
    seq = add_sequence!(scm, name)
    col === nothing || set_column!(seq, col)
    sql = col === nothing ? sql_create_sequence(seq) : sql_create_sequence(seq, col.table, col)
    @debug sql
    execute(conn, sql)
    seq
end

function create_index!(conn, scm::PGSchema, name::AbstractString, tbl::PGTable, cols::Vector{PGColumn})
    idx = add_index!(scm, name, tbl, cols)
    sql = sql_create_index(idx.name, tbl, columns)
    @debug sql
    execute(conn, sql)
    idx
end

function create_procedure!(conn, scm::PGSchema, name::AbstractString, typs::Vector{PGType}, ret_typ::PGType, lang::AbstractString, src::AbstractString)
    proc = add_procedure!(scm, name, typs, ret_typ, src)
    sql = sql_create_function(proc, proc.types, proc.return_type, lang, proc.source)
    @debug sql
    execute(conn, sql)
    proc
end

# Type operations.

function alter_name!(conn, typ::PGType, name::AbstractString)
    name != typ.name || return typ
    old_name = get_fullname(typ)
    set_name!(typ, name)
    sql = sql_rename_type(old_name, typ.name)
    @debug sql
    execute(conn, sql)
    typ
end

function alter_comment!(conn, typ::PGType, comment::Union{AbstractString,Nothing})
    comment != typ.comment || return typ
    set_comment!(typ, comment)
    sql = sql_comment_on_type(typ, typ.comment)
    @debug sql
    execute(conn, sql)
    typ
end

function drop!(conn, typ::PGType)
    scm = remove!(typ)
    sql = sql_drop_type(typ)
    @debug sql
    execute(conn, sql)
    scm
end

# Table operations.

function alter_name!(conn, tbl::PGTable, name::AbstractString)
    name != tbl.name || return tbl
    old_name = get_fullname(tbl)
    set_name!(tbl, name)
    sql = sql_rename_table(old_name, tbl.name)
    @debug sql
    execute(conn, sql)
    tbl
end

function alter_comment!(conn, tbl::PGTable, comment::Union{AbstractString,Nothing})
    comment != tbl.comment || return tbl
    set_comment!(tbl, comment)
    sql = sql_comment_on_table(tbl, tbl.comment)
    @debug sql
    execute(conn, sql)
    tbl
end

function drop!(conn, tbl::PGTable)
    scm = remove!(tbl)
    sql = sql_drop_table(tbl)
    @debug sql
    execute(conn, sql)
    scm
end

function create_column!(conn, tbl::PGTable, name::AbstractString, typ::PGType, not_null::Bool, default=nothing)
    col = add_column!(tbl, name, typ, not_null)
    default === nothing || default === missing || set_default!(col, sql_value(default))
    sql = sql_add_column(tvl, name, typ, not_null, default)
    @debug sql
    execute(conn, sql)
    col
end

function create_unique_key!(conn, tbl::PGTable, name::AbstractString, cols::Vector{PGColumn}, primary::Bool=false)
    uk = add_unique_key!(tbl, name, cols, primary)
    sql = sql_add_unique_key(uk.table, uk.name, uk.columns, uk.primary)
    @debug sql
    execute(conn, sql)
    uk
end

function create_foreign_key!(conn, tbl::PGTable, name::AbstractString, cols::Vector{PGColumn},
                             ttbl::PGTable, tcols::Vector{PGColumn},
                             on_delete::String="NO ACTION", on_update::String="NO ACTION")
    fk = add_foreign_key!(tbl, name, cols, ttbl, tcols, on_delete, on_update)
    sql = sql_add_foreign_key(fk.table, fk.name, fk.columns, fk.target_table, fk.target_columns, fk.on_delete, fk.on_update)
    @debug sql
    execute(conn, sql)
    fk
end

function create_trigger!(conn, tbl::PGTable, name::AbstractString, when::AbstractString, event::AbstractString,
                         proc::PGProcedure, args::Vector)
    tg = add_trigger!(tbl, name, proc)
    sql = sql_create_trigger(tg.table, tg.name, when, event, tg.procedure, args)
    @debug sql
    execute(conn, sql)
    tg
end

# Column operations.

function alter_name!(conn, col::PGColumn, name::AbstractString)
    name != col.name || return col
    old_name = col.name
    set_name!(col, name)
    sql = sql_rename_column(col.table, old_name, col.name)
    @debug sql
    execute(conn, sql)
    col
end

function alter_type!(conn, col::PGColumn, typ::PGType, by=nothing)
    typ != col.type_ || return col
    set_type!(col, typ)
    sql = sql_set_column_type(col.table, col, col.type_, by)
    @debug sql
    execute(conn, sql)
    col
end

function alter_not_null!(conn, col::PGColumn, not_null::Bool)
    not_null != col.not_null || return col
    set_not_null!(col, not_null)
    sql = sql_set_column_not_null(col.table, col, col.not_null)
    @debug sql
    execute(conn, sql)
    col
end

function alter_default!(conn, col::PGColumn, default)
    default != col.default && sql_value(default) != col.default || return col
    set_default!(col, default !== nothing ? sql_value(default) : nothing)
    sql = sql_set_column_default(col.table, col, col.default)
    @debug sql
    execute(conn, sql)
    col
end

function alter_comment!(conn, col::PGColumn, comment::Union{AbstractString,Nothing})
    comment != col.comment || return col
    set_comment!(col, comment)
    sql = sql_comment_on_column(col.table, col, col.comment)
    @debug sql
    execute(conn, sql)
    col
end

function drop!(conn, col::PGColumn)
    tbl = remove!(col)
    sql = sql_drop_column(col.table, col)
    @debug sql
    execute(conn, sql)
    tbl
end

# Operations on sequences.

function alter_name!(conn, seq::PGSequence, name::AbstractString)
    name != seq.name || return seq
    old_name = get_fullname(seq)
    set_name!(seq, name)
    sql = sql_rename_sequence(old_name, seq.name)
    @debug sql
    execute(conn, sql)
    seq
end

function alter_comment!(conn, seq::PGSequence, comment::Union{AbstractString,Nothing})
    comment != seq.comment || return seq
    set_comment!(seq, comment)
    sql = sql_comment_on_sequence(seq, seq.comment)
    @debug sql
    execute(conn, sql)
    seq
end

function drop!(conn, seq::PGSequence)
    scm = remove!(seq)
    sql = sql_drop_table(seq)
    @debug sql
    execute(conn, sql)
    scm
end

# Operations on indexes.

function alter_name!(conn, idx::PGIndex, name::AbstractString)
    name != idx.name || return idx
    old_name = get_fullname(idx)
    set_name!(idx, name)
    sql = sql_rename_index(old_name, idx.name)
    @debug sql
    execute(conn, sql)
    idx
end

function alter_comment!(conn, idx::PGIndex, comment::Union{AbstractString,Nothing})
    comment != idx.comment || return idx
    set_comment!(idx, comment)
    sql = sql_comment_on_index(idx, idx.comment)
    @debug sql
    execute(conn, sql)
    idx
end

function drop!(conn, idx::PGIndex)
    scm = remove!(idx)
    sql = sql_drop_index(idx)
    @debug sql
    execute(conn, sql)
    scm
end

# Operations on constraints.

function alter_name!(conn, k::Union{PGUniqueKey,PGForeignKey}, name::AbstractString)
    name != k.name || return k
    old_name = k.name
    set_name!(k, name)
    sql = sql_rename_constraint(k.table, old_name, k.name)
    @debug sql
    execute(conn, sql)
    k
end

function alter_comment!(conn, k::Union{PGUniqueKey,PGForeignKey}, comment::Union{AbstractString,Nothing})
    comment != k.comment || return k
    set_comment!(k, comment)
    sql = sql_comment_on_constraint(k.table, k, k.comment)
    @debug sql
    execute(conn, sql)
    k
end

function drop!(conn, k::Union{PGUniqueKey,PGForeignKey})
    tbl = remove!(k)
    sql = sql_drop_constraint(k.table, k)
    @debug sql
    execute(conn, sql)
    tbl
end

# Operations on procedures.

function alter_name!(conn, proc::PGProcedure, name::AbstractString)
    name != proc.name || return proc
    old_name = get_fullname(proc)
    set_name!(proc, name)
    sql = sql_rename_function(old_name, proc.types, proc.name)
    @debug sql
    execute(conn, sql)
    proc
end

function alter_comment!(conn, proc::PGProcedure, comment::Union{AbstractString,Nothing})
    comment != proc.comment || return proc
    set_comment!(proc, comment)
    sql = sql_comment_on_function(proc, proc.types, proc.comment)
    @debug sql
    execute(conn, sql)
    proc
end

function drop!(conn, proc::PGProcedure)
    scm = remove!(proc)
    sql = sql_drop_procedure(proc, proc.types)
    @debug sql
    execute(conn, sql)
    scm
end

# Operations on triggers.

function alter_name!(conn, tg::PGTrigger, name::AbstractString)
    name != tg.name || return tg
    old_name = tg.name
    set_name!(tg, name)
    sql = sql_rename_trigger(tg.table, old_name, tg.name)
    @debug sql
    execute(conn, sql)
    tg
end

function alter_comment!(conn, tg::PGTrigger, comment::Union{AbstractString,Nothing})
    comment != tg.comment || return tg
    set_comment!(tg, comment)
    sql = sql_comment_on_trigger(tg.table, tg, tg.comment)
    @debug sql
    execute(conn, sql)
    tg
end

function drop!(conn, tg::PGTrigger)
    tbl = remove!(tg)
    sql = sql_drop_trigger(tg.table, tg)
    @debug sql
    execute(conn, sql)
    tbl
end

