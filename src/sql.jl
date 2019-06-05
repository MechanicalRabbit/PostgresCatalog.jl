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

sql_name(ety::Union{PGSchema,PGColumn,PGUniqueKey,PGForeignKey,PGProcedure}) =
    sql_name(get_name(ety))

sql_name(ety::Union{PGType,PGTable,PGSequence,PGIndex,PGTrigger}) =
    sql_name(get_fullname(ety))

# Values.

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

sql_value(vals::Union{Tuple,AbstractVector}) =
    join(sql_value.(vals), ", ")

sql_value(sym::Symbol) =
    string(sym)

sql_value(ex::Expr) =
    if Meta.isexpr(ex, :call) && length(ex.args) >= 1
        "$(sql_value(ex.args[1]))($(sql_value(ex.args[2:end])))"
    elseif Meta.isexpr(ex, :(::), 2)
        "$(sql_value(ex.args[1]))::$(sql_value(ex.args[2]))"
    else
        error("cannot recognize an SQL expression: $(repr(ex))")
    end

sql_value(ref::Ref) =
    sql_name(ref[])

sql_value(ety::Union{PGSchema,PGType,PGTable,PGColumn,PGSequence,PGIndex,PGUniqueKey,PGForeignKey,PGProcedure,PGTrigger}) =
    sql_name(ety)

# Schemas.

sql_create_schema(name) =
    "CREATE SCHEMA $(sql_name(name));"

sql_rename_schema(name, new_name) =
    "ALTER SCHEMA $(sql_name(name)) RENAME TO $(sql_name(new_name));"

sql_comment_on_schema(name, comment) =
    "COMMENT ON SCHEMA $(sql_name(name)) IS $(sql_value(comment));"

sql_drop_schema(name) =
    "DROP SCHEMA $(sql_name(name));"

# Types.

sql_create_enum_type(qname, lbls) =
    "CREATE TYPE $(sql_name(qname)) AS ENUM ($(sql_value(lbls)));"

sql_rename_type(qname, new_name) =
    "ALTER TYPE $(sql_name(qname)) RENAME TO $(sql_name(new_name));"

sql_comment_on_type(qname, comment) =
    "COMMENT ON TYPE $(sql_name(qname)) IS $(sql_value(comment));"

sql_drop_type(qname) =
    "DROP TYPE $(sql_name(qname));"

# Tables.

sql_create_table(qname, clauses) =
    "CREATE TABLE $(sql_name(qname)) (\n$(join("    " .* clauses, ",\n"))\n);"

sql_rename_table(qname, new_name) =
    "ALTER TABLE $(sql_name(qname)) RENAME TO $(sql_name(new_name));"

sql_comment_on_table(qname, comment) =
    "COMMENT ON TABLE $(sql_name(qname)) IS $(sql_value(comment));"

sql_drop_table(qname) =
    "DROP TABLE $(sql_name(qname));"

# Columns.

sql_column(name, typ_qname, not_null, default=nothing) =
    "$(sql_name(name)) $(sql_name(typ_qname))$(_sql_not_null(not_null))$(_sql_default(default))"

_sql_not_null(not_null) =
    not_null ? " NOT NULL" : ""

_sql_default(default) =
    default !== nothing && default !== missing ? " $(sql_value(default))" : ""

sql_add_column(tbl_qname, name, typ_qname, not_null, default=nothing) =
    "ALTER TABLE $(sql_name(tbl_qname)) ADD COLUMN $(_sql_column(name, typ_qname, not_null, default));"

sql_rename_column(tbl_qname, name, new_name) =
    "ALTER TABLE $(sql_name(tbl_qname)) RENAME COLUMN $(sql_name(name)) TO $(sql_name(new_name));"

sql_copy_column(tbl_qname, name, src_name) =
    "UPDATE $(sql_name(tbl_qname)) SET $(sql_name(name)) = $(sql_name(src_name));"

sql_set_column_type(tbl_qname, name, typ_qname, by=nothing) =
    "ALTER TABLE $(sql_name(tbl_qname)) ALTER COLUMN $(sql_name(name)) SET DATA TYPE $(sql_name(typ_qname))$(_sql_using(by));"

_sql_using(by) =
    by !== nothing && by !== missing ? " USING $(sql_value(by))" : ""

sql_set_column_not_null(tbl_qname, name, not_null) =
    "ALTER TABLE $(sql_name(tbl_qname)) ALTER COLUMN $(sql_name(name))$(_sql_set_not_null(not_null));"

_sql_set_not_null(not_null) =
    not_null ? " SET NOT NULL" : "DROP NOT NULL"

sql_set_column_default(tbl_qname, name, default) =
    "ALTER TABLE $(sql_name(tbl_qname)) ALTER COLUMN $(sql_name(name))$(_sql_set_default(default));"

_sql_set_default(default) =
    default !== nothing && default !== mising ? " SET DEFAULT $(sql_value(default))" : " DROP DEFAULT"

sql_comment_on_column(tbl_qname, name, comment) =
    "COMMENT ON COLUMN $(sql_name(tbl_qname)).$(sql_name(name)) IS $(sql_value(comment));"

sql_drop_column(tbl_qname, name) =
    "ALTER TABLE $(sql_name(tbl_qname)) DROP COLUMN $(sql_name(name));"

# Sequences.

sql_create_sequence(qname, tbl_qname=nothing, col_name=nothing) =
    "CREATE SEQUENCE $(sql_name(qname))$(_sql_owned_by(tbl_qname, col_name));"

_sql_owned_by(tbl_qname, col_name) =
    tbl_qname !== nothing && col_name !== nothing ? " OWNED BY $(sql_name(tbl_qname)).$(sql_name(col_name))" : ""

sql_rename_sequence(qname, new_name) =
    "ALTER SEQUENCE $(sql_name(qname)) RENAME TO $(sql_name(new_name));"

sql_comment_on_sequence(qname, comment) =
    "COMMENT ON SEQUENCE $(sql_name(qname)) IS $(sql_value(comment));"

sql_drop_sequence(qname) =
    "DROP SEQUENCE $(sql_name(qname));"

# Indexes.

sql_create_index(name, tbl_name, col_names) =
    "CREATE INDEX $(sql_name(name)) ON $(sql_name(tbl_name)) ($(sql_name(col_names)));"

sql_rename_index(qname, new_name) =
    "ALTER INDEX $(sql_name(qname)) RENAME TO $(sql_name(new_name));"

sql_comment_on_index(qname, comment) =
    "COMMENT ON INDEX $(sql_name(qname)) IS $(sql_value(comment));"

sql_drop_index(qname) =
    "DROP INDEX $(sql_name(qname));"

# Constraints.

sql_add_unique_constraint(tbl_qname, name, col_names, primary=false) =
    "ALTER TABLE $(sql_name(tbl_qname)) ADD CONSTRAINT $(sql_name(name))$(_sql_primary(primary)) ($(sql_name(col_names)));"

_sql_primary(primary) =
    primary ? " PRIMARY KEY" : " UNIQUE"

sql_add_foreign_key_constraint(tbl_qname, name, col_names, ttbl_qname, tcol_names, on_delete=nothing, on_update=nothing) =
    "ALTER TABLE $(sql_name(tbl_qname)) ADD CONSTRAINT $(sql_name(name)) FOREIGN KEY $(sql_name(col_names)) REFERENCES $(sql_name(ttbl_name)) ($(sql_name(tcol_names)))$(_sql_on_delete(on_delete))$(_sql_on_update(on_update));"

_sql_on_delete(on_delete) =
    on_delete !== nothing ? " ON DELETE $on_delete" : ""

_sql_on_update(on_update) =
    on_update !== nothing ? " ON UPDATE $on_update" : ""

sql_rename_constraint(tbl_qname, name, new_name) =
    "ALTER TABLE $(sql_name(tbl_qname)) RENAME CONSTRAINT $(sql_name(name)) TO $(sql_name(new_name));"

sql_comment_on_constraint(tbl_qname, name, comment) =
    "COMMENT ON CONSTRAINT $(sql_name(name)) ON $(sql_name(tbl_qname)) IS $(sql_value(comment));"

sql_drop_constraint(tbl_qname, name) =
    "ALTER TABLE $(sql_name(tbl_qname)) DROP CONSTRAINT $(sql_name(name));"

# Procedures.

sql_create_function(qname, typ_names, ret_typ_name, lang, src) =
    "CREATE OR REPLACE FUNCTION $(sql_name(qname))($(sql_name(typ_names))) RETURNS $(sql_name(ret_typ_name)) LANGUAGE $lang AS $(sql_value(src));"

sql_rename_function(qname, typ_names, new_name) =
    "ALTER FUNCTION $(sql_name(qname))($(sql_name(typ_names))) RENAME TO $(sql_name(new_name));"

sql_comment_on_function(qname, typ_names, comment) =
    "COMMENT ON FUNCTION $(sql_name(qname))($(sql_name(typ_names))) IS $(sql_value(comment));"

sql_drop_function(qname, typ_names) =
    "DROP FUNCTION $(sql_name(qname))($(sql_name(typ_names)));"

# Triggers.

sql_create_trigger(tbl_qname, name, when, event, proc_name, args) =
    "CREATE TRIGGER $(sql_name(name)) $when $event ON $(sql_name(tbl_qname)) FOR EACH ROW EXECUTE PROCEDURE $(sql_name(proc_name))($(join(args, ", ")));"

sql_rename_trigger(tbl_qname, name, new_name) =
    "ALTER TRIGGER $(sql_name(name)) ON $(sql_name(tbl_qname)) RENAME TO $(sql_name(new_name));"

sql_comment_on_trigger(tbl_qname, name, comment) =
    "COMMENT ON TRIGGER $(sql_name(name)) ON $(sql_name(tbl_qname)) IS $(sql_value(comment));"

sql_drop_trigger(tbl_qname, name) =
    "DROP TRIGGER $(sql_name(name)) ON $(sql_name(tbl_qname));"

