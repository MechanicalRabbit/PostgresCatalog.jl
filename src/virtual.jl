#
# Virtual properties.
#

@inline Base.getproperty(cat::PGCatalog, prop::Symbol) =
    if prop === :schema
        (name, default...) -> get_schema(cat, name, default...)
    elseif prop === :schemas
        () -> list_schemas(cat)
    else
        getfield(cat, prop)
    end

@inline Base.getproperty(scm::PGSchema, prop::Symbol) =
    if prop === :catalog
        get_catalog(scm)
    elseif prop === :fullname
        get_fullname(scm)
    elseif prop === :type_
        (name, default...) -> get_type(scm, name, default...)
    elseif prop === :types
        () -> list_types(scm)
    elseif prop === :table
        (name, default...) -> get_table(scm, name, default...)
    elseif prop === :tables
        () -> list_tables(scm)
    else
        getfield(scm, prop)
    end

@inline Base.getproperty(typ::PGType, prop::Symbol) =
    if prop === :schema
        get_schema(typ)
    elseif prop === :fullname
        get_fullname(typ)
    elseif prop === :labels
        get_labels(typ)
    elseif prop === :columns
        () -> list_columns(typ)
    else
        getfield(typ, prop)
    end

@inline Base.getproperty(tbl::PGTable, prop::Symbol) =
    if prop === :schema
        get_schema(tbl)
    elseif prop === :fullname
        get_fullname(tbl)
    elseif prop === :column
        (name, default...) -> get_column(tbl, name, default...)
    elseif prop === :columns
        () -> list_columns(tbl)
    else
        getfield(tbl, prop)
    end

@inline Base.getproperty(col::PGColumn, prop::Symbol) =
    if prop === :table
        get_table(col)
    elseif prop === :type_
        get_type(col)
    else
        getfield(col, prop)
    end

