#
# Defining mutually-recursive types.
#

macro rectypes(blk)
    if Meta.isexpr(blk, :block)
        names = Symbol[]
        for def in blk.args
            if Meta.isexpr(def, :macrocall) && length(def.args) > 1 && def.args[1] == GlobalRef(Core, Symbol("@doc"))
                def = def.args[end]
            end
            if Meta.isexpr(def, :struct, 3) && def.args[2] isa Symbol
                push!(names, def.args[2])
            end
        end
        name2deps = Dict{Symbol,Vector{Symbol}}()
        for def in blk.args
            if Meta.isexpr(def, :macrocall) && length(def.args) > 1 && def.args[1] == GlobalRef(Core, Symbol("@doc"))
                def = def.args[end]
            end
            if Meta.isexpr(def, :struct, 3) && def.args[2] isa Symbol
                name = def.args[2]
                deps = name2deps[name] = Symbol[]
                collect_deps!(name, name2deps, names, def)
                rewrite_deps!(name2deps, def)
            end
        end
        for name in reverse(names)
            repl = Symbol("$(name)_")
            deps = name2deps[name]
            push!(blk.args, Expr(:const, Expr(:(=), name, !isempty(deps) ? Expr(:curly, repl, deps...) : repl)))
            push!(blk.args, :(Base.show_datatype(io::IO, ::Type{$name}) = print(io, $(QuoteNode(name)))))
        end
    end
    return esc(blk)
end

collect_deps!(name, name2deps, names, ex) =
    if ex isa Expr
        for arg in ex.args
            collect_deps!(name, name2deps, names, arg)
        end
    elseif ex isa Symbol && ex in names && !(ex in name2deps[name])
        if ex in keys(name2deps)
            for dep in name2deps[ex]
                collect_deps!(name, name2deps, names, dep)
            end
        elseif ex != name
            push!(name2deps[name], ex)
        end
    end

rewrite_deps!(name2deps, ex) =
    if ex isa Expr
        for (i, arg) in enumerate(ex.args)
            if arg isa Symbol && arg in keys(name2deps)
                repl = Symbol("$(arg)_")
                deps = name2deps[arg]
                ex.args[i] = !isempty(deps) ? Expr(:curly, repl, deps...) : repl
                rewrite_deps!(name2deps, ex.args[i])
            else
                fn = Meta.isexpr(ex, (:function, :(=)), 2) &&
                     i == 1 &&
                     Meta.isexpr(arg, :call) &&
                     length(arg.args) >= 1 &&
                     !Meta.isexpr(arg.args[1], :curly)
                rewrite_deps!(name2deps, arg)
                if fn && Meta.isexpr(arg.args[1], :curly)
                    ex.args[i] = Expr(:where, arg, arg.args[1].args[2:end]...)
                end
            end
        end
    end

