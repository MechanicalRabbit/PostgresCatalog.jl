#
# SQL fragments.
#

# Names.

sql_name(name::AbstractString) =
    "\"$(replace(name, "\"" => "\"\""))\""

_in_search_path(qname) =
    qname[1] == "pg_catalog" || qname[1] == "public"

sql_name(qname::Tuple) =
    join(sql_name.(_in_search_path(qname) ? Base.tail(qname) : qname), '.')

sql_name(names::AbstractVector) =
    join(sql_name.(names), ", ")

