# PostgresCatalog.jl


## Overview

`PostgresCatalog` is a Julia library for introspecting and manipulating the
structure of a Postgres database.


### Installation

Use the Julia package manager.

```julia
julia> using Pkg
julia> Pkg.add("PostgresCatalog")
```


### Using `PostgresCatalog`

    using PostgresCatalog
    using LibPQ

    conn = LibPQ.Connection("")

    cat = introspect(conn)
    #-> <DATABASE>

    scm = cat["public"]
    #-> <SCHEMA "public">


## API Reference

```@docs
PostgresCatalog.introspect
```


## Test Suite

