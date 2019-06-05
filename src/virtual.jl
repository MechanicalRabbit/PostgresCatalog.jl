#
# Virtual properties and methods.
#

@inline Base.getproperty(cat::PGCatalog, prop::Symbol) =
    if prop === :create_schema!
        (conn, name) -> create_schema!(conn, cat, name)
    else
        getfield(cat, prop)
    end

@inline Base.getproperty(scm::PGSchema, prop::Symbol) =
    if prop === :fullname
        get_fullname(scm)
    elseif prop === :alter_name!
        (conn, name) -> alter_name!(conn, scm, name)
    elseif prop === :alter_comment!
        (conn, comment) -> alter_comment!(conn, scm, comment)
    elseif prop === :drop!
        conn -> drop!(conn, scm)
    elseif prop === :create_type!
        (conn, name, lbls) -> create_type!(conn, scm, name, lbls)
    elseif prop === :create_table!
        (conn, name, cols) -> create_table!(conn, scm, name, cols)
    elseif prop === :create_sequence!
        (conn, name, col=nothing) -> create_sequence!(conn, scm, name, col)
    elseif prop === :create_index!
        (conn, name, tbl, cols) -> create_index!(conn, scm, name, tbl, cols)
    elseif prop === :create_procedure!
        (conn, name, typs, ret_typ, lang, src) -> create_procedure!(conn, scm, name, typs, ret_typ, lang, src)
    else
        getfield(scm, prop)
    end

@inline Base.getproperty(typ::PGType, prop::Symbol) =
    if prop === :fullname
        get_fullname(typ)
    elseif prop === :alter_name!
        (conn, name) -> alter_name!(conn, typ, name)
    elseif prop === :alter_comment!
        (conn, comment) -> alter_comment!(conn, typ, comment)
    elseif prop === :drop!
        conn -> drop!(conn, typ)
    else
        getfield(typ, prop)
    end

@inline Base.getproperty(tbl::PGTable, prop::Symbol) =
    if prop === :fullname
        get_fullname(tbl)
    elseif prop === :alter_name!
        (conn, name) -> alter_name!(conn, tbl, name)
    elseif prop === :alter_comment!
        (conn, comment) -> alter_comment!(conn, tbl, comment)
    elseif prop === :drop!
        conn -> drop!(conn, tbl)
    elseif prop === :create_column!
        (conn, name, typ, not_null, default=nothing) -> create_column!(conn, tbl, typ, not_null, default)
    elseif prop === :create_unique_key!
        (conn, name, cols, primary=false) -> create_unique_key!(conn, tbl, name, cols, primary)
    elseif prop === :create_foreign_key!
        (conn, name, cols, ttbl, tcols, on_delete="NO ACTION", on_update="NO_ACTION") ->
            create_foreign_key!(conn, tbl, name, cols, ttbl, tcols, on_delete, on_update)
    elseif prop === :create_trigger!
        (conn, name, when, event, proc, args) -> create_trigger!(conn, tbl, name, when, event, proc, args)
    else
        getfield(tbl, prop)
    end

@inline Base.getproperty(col::PGColumn, prop::Symbol) =
    if prop === :fullname
        get_fullname(col)
    elseif prop === :alter_name!
        (conn, name) -> alter_name!(conn, col, name)
    elseif prop === :alter_type!
        (conn, typ) -> alter_type!(conn, col, typ)
    elseif prop === :alter_is_null!
        (conn, is_null) -> alter_type!(conn, col, is_null)
    elseif prop === :alter_default!
        (conn, default) -> alter_type!(conn, col, default)
    elseif prop === :alter_comment!
        (conn, comment) -> alter_comment!(conn, col, comment)
    elseif prop === :drop!
        conn -> drop!(conn, col)
    else
        getfield(col, prop)
    end

@inline Base.getproperty(seq::PGSequence, prop::Symbol) =
    if prop === :fullname
        get_fullname(seq)
    elseif prop === :alter_name!
        (conn, name) -> alter_name!(conn, seq, name)
    elseif prop === :alter_comment!
        (conn, comment) -> alter_comment!(conn, seq, comment)
    elseif prop === :drop!
        conn -> drop!(conn, seq)
    else
        getfield(seq, prop)
    end

@inline Base.getproperty(idx::PGIndex, prop::Symbol) =
    if prop === :fullname
        get_fullname(idx)
    elseif prop === :alter_name!
        (conn, name) -> alter_name!(conn, idx, name)
    elseif prop === :alter_comment!
        (conn, comment) -> alter_comment!(conn, idx, comment)
    elseif prop === :drop!
        conn -> drop!(conn, idx)
    else
        getfield(idx, prop)
    end

@inline Base.getproperty(uk::PGUniqueKey, prop::Symbol) =
    if prop === :fullname
        get_fullname(uk)
    elseif prop === :alter_name!
        (conn, name) -> alter_name!(conn, uk, name)
    elseif prop === :alter_comment!
        (conn, comment) -> alter_comment!(conn, uk, comment)
    elseif prop === :drop!
        conn -> drop!(conn, uk)
    else
        getfield(uk, prop)
    end

@inline Base.getproperty(fk::PGForeignKey, prop::Symbol) =
    if prop === :fullname
        get_fullname(fk)
    elseif prop === :alter_name!
        (conn, name) -> alter_name!(conn, fk, name)
    elseif prop === :alter_comment!
        (conn, comment) -> alter_comment!(conn, fk, comment)
    elseif prop === :drop!
        conn -> drop!(conn, fk)
    else
        getfield(uk, prop)
    end

@inline Base.getproperty(proc::PGProcedure, prop::Symbol) =
    if prop === :fullname
        get_fullname(proc)
    elseif prop === :alter_name!
        (conn, name) -> alter_name!(conn, proc, name)
    elseif prop === :alter_comment!
        (conn, comment) -> alter_comment!(conn, proc, comment)
    elseif prop === :drop!
        conn -> drop!(conn, proc)
    else
        getfield(proc, prop)
    end

@inline Base.getproperty(tg::PGTrigger, prop::Symbol) =
    if prop === :fullname
        get_fullname(tg)
    elseif prop === :alter_name!
        (conn, name) -> alter_name!(conn, tg, name)
    elseif prop === :alter_comment!
        (conn, comment) -> alter_comment!(conn, tg, comment)
    elseif prop === :drop!
        conn -> drop!(conn, tg)
    else
        getfield(tg, prop)
    end

