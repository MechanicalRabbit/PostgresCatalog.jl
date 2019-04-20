#
# Introspecting and managing PostgreSQL catalog.
#

module PostgresCatalog

using LibPQ

include("sql.jl")
include("rectypes.jl")
include("entities.jl")
include("virtual.jl")
include("introspect.jl")

end
