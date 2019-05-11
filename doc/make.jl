#!/usr/bin/env julia

using Pkg
haskey(Pkg.installed(), "Documenter") || Pkg.add("Documenter")

using Documenter
using PostgresCatalog

# Highlight indented code blocks as Julia code.
using Markdown
Markdown.Code(code) = Markdown.Code("julia", code)

makedocs(
    sitename = "PostgresCatalog.jl",
    pages = [
        "Home" => "index.md",
    ],
    modules = [PostgresCatalog])

deploydocs(
    repo = "github.com/rbt-lang/PostgresCatalog.jl.git",
)
