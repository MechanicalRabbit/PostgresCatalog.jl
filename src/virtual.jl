#
# Virtual properties and methods.
#

@inline Base.getproperty(cat::PGCatalog, prop::Symbol) =
    getfield(cat, prop)

@inline Base.getproperty(scm::PGSchema, prop::Symbol) =
    if prop === :fullname
        get_fullname(scm)
    else
        getfield(scm, prop)
    end

@inline Base.getproperty(typ::PGType, prop::Symbol) =
    if prop === :fullname
        get_fullname(typ)
    else
        getfield(typ, prop)
    end

@inline Base.getproperty(proc::PGProcedure, prop::Symbol) =
    if prop === :fullname
        get_fullname(proc)
    else
        getfield(proc, prop)
    end

@inline Base.getproperty(tbl::PGTable, prop::Symbol) =
    if prop === :fullname
        get_fullname(tbl)
    else
        getfield(tbl, prop)
    end

@inline Base.getproperty(col::PGColumn, prop::Symbol) =
    if prop === :fullname
        get_fullname(col)
    else
        getfield(col, prop)
    end

@inline Base.getproperty(seq::PGSequence, prop::Symbol) =
    if prop === :fullname
        get_fullname(seq)
    else
        getfield(seq, prop)
    end

