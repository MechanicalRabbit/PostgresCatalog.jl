#
# SQL fragments.
#

sql_name(name::AbstractString) =
    "\"$(replace(name, "\"" => "\"\""))\""

_search_path(qname) =
    qname[1] == "pg_catalog" || qname[1] == "public"

sql_name(qname::Tuple) =
    join(sql_name.(_search_path(qname) ? Base.tail(qname) : qname), '.')

sql_name(names::Vector) =
    join(sql_name.(names), ", ")

sql_value(::Union{Nothing,Missing}) =
    "NULL"

sql_value(val::Bool) =
    val ? "TRUE" : "FALSE"

sql_value(val::Number) =
    "$val"

function sql_value(val::AbstractString)
    val = replace(val, '\'' => "''")
    if '\\' in val  # compatibility with standard_conforming_strings=off
        val = replace(val, '\\' => "\\\\")
        val = "E'$val'"
    else
        val = "'$val'"
    end
    val
end

sql_value(vals::Union{Tuple,Vector}) =
    join(sql_value.(vals), ", ")

sql_create_schema(name::AbstractString) =
    "CREATE SCHEMA $(sql_name(name));"

sql_rename_schema(name::AbstractString, new_name::AbstractString) =
    "ALTER SCHEMA $(sql_name(name)) RENAME TO $(sql_name(new_name));"

sql_comment_on_schema(name, comment) =
    "COMMENT ON SCHEMA $(sql_name(name)) IS $(sql_value(comment));"

sql_drop_schema(name::AbstractString) =
    "DROP SCHEMA $(sql_name(name));"

