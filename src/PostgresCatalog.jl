#
# Introspecting and managing PostgreSQL catalog.
#

module PostgresCatalog

using LibPQ
using Tables

include("rectypes.jl")
include("entities.jl")
include("virtual.jl")
include("introspect.jl")
include("ddl.jl")
include("sql.jl")

end
