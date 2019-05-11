#
# Introspecting and managing PostgreSQL catalog.
#

module PostgresCatalog

export introspect

using LibPQ
using Tables

include("sql.jl")
include("rectypes.jl")
include("entities.jl")
include("virtual.jl")
include("introspect.jl")

end
