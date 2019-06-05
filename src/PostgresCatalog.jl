#
# Introspecting and managing PostgreSQL catalog.
#

module PostgresCatalog

using LibPQ
using Tables

include("rectypes.jl")
include("entities.jl")
include("introspect.jl")
include("sql.jl")

end
