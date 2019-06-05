#!/usr/bin/env julia

using PostgresCatalog
using NarrativeTest
using Logging

struct TestLogger <: AbstractLogger
    fallback
end

function Logging.handle_message(logger::TestLogger, level, message, _module, group, id, file, line; kws...)
    if _module == PostgresCatalog
        lines = split(string(message), '\n')
        println("┌ Executing SQL:")
        for line in lines[1:end-1]
            println("│ ", line)
        end
        println("└ ", lines[end])
    else
        Logging.handle_message(logger.fallback, level, message, _module, group, id, file, line; kws...)
    end
end

function Logging.shouldlog(logger::TestLogger, level, _module, group, id)
    _module == PostgresCatalog && level >= Logging.Debug ||
    level >= Logging.min_enabled_level(logger.fallback) &&
    Logging.shouldlog(logger.fallback, level, _module, group, id)
end

function Logging.min_enabled_level(logger::TestLogger)
    Logging.Debug
end

global_logger(TestLogger(global_logger()))

args = !isempty(ARGS) ? ARGS : [relpath(joinpath(dirname(abspath(PROGRAM_FILE)), "../doc/src"))]
exit(!runtests(args))
