macro genstruct(x)
    return :(mutable struct $x end)
end

@genstruct CassFuture
@genstruct CassCluster
@genstruct CassSession
@genstruct CassString
@genstruct CassStatement
@genstruct CassResult
@genstruct CassIterator
@genstruct CassRow
@genstruct CassValue
@genstruct CassPrepared
@genstruct CassBatch

function cql_future_error_code(future::Ptr{CassFuture})
    val = ccall(
            (:cass_future_error_code, :libcassandra),
            UInt16,
            (Ptr{CassFuture},),
            future)
    return val::UInt16
end

function cql_future_error_message(future::Ptr{CassFuture}, strref::Ref{Ptr{UInt8}}, siz::Ptr{Void})
    ccall(
        (:cass_future_error_message, :libcassandra),
        Void,
        (Ptr{CassFuture}, Ref{Ptr{UInt8}}, Ref{Csize_t}),
        future, strref, siz)
end

function cql_cluster_new()
    val = ccall(
            (:cass_cluster_new, :libcassandra),
            Ptr{CassCluster},
            ())
    return val::Ptr{CassCluster}
end

function cql_session_new()
    val = ccall(
            (:cass_session_new, :libcassandra),
            Ptr{CassSession},
            ())
    return val::Ptr{CassSession}
end

function cql_cluster_set_contact_points(cluster::Ptr{CassCluster}, hosts::String)
    ccall(
        (:cass_cluster_set_contact_points, :libcassandra),
        Void,
        (Ptr{CassCluster}, Cstring),
        cluster, hosts)
end

function cql_session_connect(session::Ptr{CassSession}, cluster::Ptr{CassCluster})
    val = ccall(
            (:cass_session_connect, :libcassandra),
            Ptr{CassFuture},
            (Ptr{CassSession}, Ptr{CassCluster}),
            session, cluster)
    return val::Ptr{CassFuture}
end

function cql_session_free(session::Ptr{CassSession})
    ccall(
        (:cass_session_free, :libcassandra),
        Void,
        (Ptr{CassCluster},),
        session)
end

function cql_cluster_free(cluster::Ptr{CassCluster})
    ccall(
        (:cass_cluster_free, :libcassandra),
        Void,
        (Ptr{CassCluster},),
        cluster)
end

function cql_result_row_count(result::Ptr{CassResult})
    val = ccall(
            (:cass_result_row_count, :libcassandra),
            Int32,
            (Ptr{CassResult},),
            result)
    return val::Int32
end

function cql_result_column_count(result::Ptr{CassResult})
    val = ccall(
            (:cass_result_column_count, :libcassandra),
            Int32,
            (Ptr{CassResult},),
            result)
    return val::Int32
end

function cql_iterator_next(iterator::Ptr{CassIterator})
    next = ccall(
            (:cass_iterator_next, :libcassandra),
            UInt8,
            (Ptr{CassIterator},),
            iterator)
    val = ifelse(next == 0, false, true)
    return val::Bool
end

function cql_future_free(future::Ptr{CassFuture})
    ccall(
        (:cass_future_free, :libcassandra),
        Void,
        (Ptr{CassFuture},),
        future)
end

function cql_result_column_type(result::Ptr{CassResult}, idx::Int64)
    val = ccall(
            (:cass_result_column_type, :libcassandra),
            UInt16,
            (Ptr{CassResult}, UInt32),
            result, idx)
    return val::UInt16
end

function cql_value_get_int64(val::Ptr{CassValue}, out::Ref{Clonglong})
    err = ccall(
            (:cass_value_get_int64, :libcassandra),
            Cushort,
            (Ptr{CassValue}, Ref{Clonglong}),
            val, out)
    return err::UInt16
end

function cql_value_get_int32(val::Ptr{CassValue}, out::Ref{Cint})
    err = ccall(
            (:cass_value_get_int32, :libcassandra),
            Cushort,
            (Ptr{CassValue}, Ref{Cint}),
            val, out)
    return err::UInt16
end

function cql_value_get_string(val::Ptr{CassValue}, out::Ref{Ptr{UInt8}}, siz::Ptr{Void})
    err = ccall(
            (:cass_value_get_string, :libcassandra),
            Cushort,
            (Ptr{CassValue}, Ref{Ptr{UInt8}}, Ref{Csize_t}),
            val, out, siz)
    return err::UInt16
end

function cql_value_get_float(val::Ptr{CassValue}, out::Ref{Cdouble})
    err = ccall(
            (:cass_value_get_double, :libcassandra),
            Cushort,
            (Ptr{CassValue}, Ref{Cdouble}),
            val, out)
    return err::UInt16
end

function cql_statement_free(statement::Ptr{CassStatement})
    ccall(
        (:cass_statement_free, :libcassandra),
        Void,
        (Ptr{CassStatement},),
        statement)
end

function cql_result_free(result::Ptr{CassResult})
    ccall(
        (:cass_result_free, :libcassandra),
        Void,
        (Ptr{CassResult},),
        result)
end

function cql_iterator_free(iterator::Ptr{CassIterator})
    ccall(
        (:cass_iterator_free, :libcassandra),
        Void,
        (Ptr{CassIterator},),
        iterator)
end

function cql_statement_new(query::String)
    statement = ccall(
                    (:cass_statement_new, :libcassandra),
                    Ptr{CassStatement},
                    (Cstring, Int32),
                    query, 0)
    return statement::Ptr{CassStatement}
end

function cql_statement_set_paging_size(statement::Ptr{CassStatement}, pgsize::Int)
    ccall(
        (:cass_statement_set_paging_size, :libcassandra),
        Void,
        (Ptr{CassStatement}, Cint),
        statement, pgsize)
end

function cql_session_execute(session::Ptr{CassSession}, statement::Ptr{CassStatement})
    future = ccall(
                (:cass_session_execute, :libcassandra),
                Ptr{CassFuture},
                (Ptr{CassSession}, Ptr{CassStatement}),
                session, statement)
    return future::Ptr{CassFuture}
end

function cql_future_get_result(future::Ptr{CassFuture})
    result = ccall(
                (:cass_future_get_result, :libcassandra),
                Ptr{CassResult},
                (Ptr{CassFuture},),
                future)
    return result::Ptr{CassResult}
end

function cql_iterator_from_result(result::Ptr{CassResult})
    iterator = ccall(
                (:cass_iterator_from_result, :libcassandra),
                Ptr{CassIterator},
                (Ptr{CassResult},),
                result)
    return iterator::Ptr{CassIterator}
end

function cql_iterator_get_row(iterator::Ptr{CassIterator})
    row = ccall(
            (:cass_iterator_get_row, :libcassandra),
            Ptr{CassRow},
            (Ptr{CassIterator},),
            iterator)
    return row::Ptr{CassRow}
end

function cql_row_get_column(row::Ptr{CassRow}, pos::Int64)
    val = ccall(
            (:cass_row_get_column, :libcassandra),
            Ptr{CassValue},
            (Ptr{CassRow}, Clonglong),
            row, pos)
    return val::Ptr{CassValue}
end

function cql_result_has_more_pages(result::Ptr{CassResult})
    hasmore = ccall(
                (:cass_result_has_more_pages, :libcassandra),
                Cint,
                (Ptr{CassResult},),
                result)
    out = convert(Bool, hasmore)
    return out::Bool
end

function cql_statement_set_paging_state(statement::Ptr{CassStatement}, result::Ptr{CassResult})
    ccall(
        (:cass_statement_set_paging_state, :libcassandra),
        Void,
        (Ptr{CassStatement}, Ptr{CassResult}),
        statement, result)
end

function cql_future_wait(future::Ptr{CassFuture})
    ccall(
        (:cass_future_wait, :libcassandra),
        Void,
        (Ptr{CassFuture},),
        future)
end

function cql_session_prepare(session::Ptr{CassSession}, query::String)
    future = ccall(
                (:cass_session_prepare, :libcassandra),
                Ptr{CassFuture},
                (Ptr{CassSession}, Cstring),
                session, query)
    return future::Ptr{CassFuture}
end

function cql_batch_new(batch_type::UInt8)
    #=
    CASS_BATCH_TYPE_LOGGED = 0x00 
    CASS_BATCH_TYPE_UNLOGGED = 0x01 
    CASS_BATCH_TYPE_COUNTER = 0x02 
    =#
    batch = ccall(
                (:cass_batch_new, :libcassandra),
                Ptr{CassBatch},
                (Cuchar,),
                batch_type)
    return batch::Ptr{CassBatch}
end

function cql_future_get_prepared(future::Ptr{CassFuture})
    prep = ccall(
            (:cass_future_get_prepared, :libcassandra),
            Ptr{CassPrepared},
            (Ptr{CassFuture},),
            future)
    return prep::Ptr{CassPrepared}
end

function cql_prepared_bind(prep::Ptr{CassPrepared})
    statement = ccall(
                    (:cass_prepared_bind, :libcassandra),
                    Ptr{CassStatement},
                    (Ptr{CassPrepared},),
                    prep)
    return statement::Ptr{CassStatement}
end

function cql_statement_bind_string(statement::Ptr{CassStatement}, pos::Int, data::String)
    ccall(
        (:cass_statement_bind_string, :libcassandra),
        Void,
        (Ptr{CassStatement}, Cint, Cstring),
        statement, pos, data)
end

function cql_statement_bind_int32(statement::Ptr{CassStatement}, pos::Int, data::Int32)
    ccall(
        (:cass_statement_bind_int32, :libcassandra),
        Void,
        (Ptr{CassStatement}, Cint, Cint),
        statement, pos, data)
end

function cql_statement_bind_int64(statement::Ptr{CassStatement}, pos::Int, data::Int64)
    ccall(
        (:cass_statement_bind_int64, :libcassandra),
        Void,
        (Ptr{CassStatement}, Cint, Clonglong),
        statement, pos, data)
end

function cql_statement_bind_double(statement::Ptr{CassStatement}, pos::Int, data::Float64)
    ccall(
        (:cass_statement_bind_double, :libcassandra),
        Void,
        (Ptr{CassStatement}, Cint, Cdouble),
        statement, pos, data)
end

function cql_batch_add_statement(batch::Ptr{CassBatch}, statement::Ptr{CassStatement})
    ccall(
        (:cass_batch_add_statement, :libcassandra),
        Void,
        (Ptr{CassBatch}, Ptr{CassStatement}),
        batch, statement)
end

function cql_session_execute_batch(session::Ptr{CassSession}, batch::Ptr{CassBatch})
    future = ccall(
                (:cass_session_execute_batch, :libcassandra),
                Ptr{CassFuture},
                (Ptr{CassSession}, Ptr{CassBatch}),
                session, batch)
    return future::Ptr{CassFuture}
end

function cql_batch_free(batch::Ptr{CassBatch})
    ccall(
        (:cass_batch_free, :libcassandra),
        Void,
        (Ptr{CassBatch},),
        batch)
end

function cql_prepared_free(prep::Ptr{CassPrepared})
    ccall(
        (:cass_prepared_free, :libcassandra),
        Void,
        (Ptr{CassPrepared},),
        prep)
end

