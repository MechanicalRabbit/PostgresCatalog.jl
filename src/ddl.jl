#
# Manipulations.
#

function create_schema!(conn, cat::PGCatalog, name::AbstractString)
    sql = sql_create_schema(name)
    execute(conn, sql)
    add_schema!(cat, name)
end

function alter_name!(conn, scm::PGSchema, name::AbstractString)
    @assert scm.linked
    name != scm.name || return scm
    sql = sql_rename_schema(scm.name, name)
    execute(conn, sql)
    set_name!(scm, name)
end

function alter_comment!(conn, scm::PGSchema, comment::Union{AbstractString,Nothing})
    @assert scm.linked
    comment != scm.comment || return scm
    sql = sql_comment_on_schema(scm.name, comment)
    execute(conn, sql)
    set_comment!(scm, comment)
end

function drop!(conn, scm::PGSchema)
    @assert scm.linked
    sql = sql_drop_schema(scm.name)
    execute(conn, sql)
    remove!(scm)
end



