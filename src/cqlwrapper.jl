using Base.Libc

# Cassandra uses a different ordering of the 8 bytes of UUIDs than Julia does, so we need conversion utilities.
struct CassUuid
    underlying_bytes::NTuple{16,UInt8}
end
CassUuid(uuid::UUID) = CassUuid(reinterpret(NTuple{16, UInt8}, [uuid.value])[1][cat(13:16, 11:12, 9:10, 1:8, dims=1)])
UUID(cass_uuid::CassUuid) = UUID(reinterpret(UInt128, [cass_uuid.underlying_bytes[cat(9:16, 7:8, 5:6, 1:4, dims=1)]])[1])

Base.convert(::Type{CassUuid}, uuid::UUID) = CassUuid(uuid)
Base.convert(::Type{UUID}, cass_uuid::CassUuid) = UUID(cass_uuid)

get_null_cass_uuid_ref()::Ref{CassUuid} = Ref(CassUuid(NTuple{16, UInt8}(0x00 for i in 1:16)))

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
@genstruct CassUuidGen

function cql_cluster_set_concurrency(cluster::Ptr{CassCluster}, nthreads::Int64)
    val = ccall(
            (:cass_cluster_set_num_threads_io, "libcassandra.so.2"),
            UInt16,
            (Ptr{CassCluster}, UInt32),
            cluster, nthreads)
    return val::UInt16
end

function cql_cluster_set_connections_per_host(cluster::Ptr{CassCluster}, siz::Int64)
    val1 = ccall(
        (:cass_cluster_set_max_connections_per_host, "libcassandra.so.2"),
        UInt16,
        (Ptr{CassCluster}, UInt32),
        cluster, siz)
    val2 = ccall(
        (:cass_cluster_set_core_connections_per_host, "libcassandra.so.2"),
        UInt16,
        (Ptr{CassCluster}, UInt32),
        cluster, siz)
    val = val1 | val2
    return val::UInt16
end

function cql_cluster_set_write_bytes_high_water_mark(cluster::Ptr{CassCluster}, siz::Int64)
    val = ccall(
        (:cass_cluster_set_write_bytes_high_water_mark, "libcassandra.so.2"),
        UInt16,
        (Ptr{CassCluster}, UInt32),
        cluster, siz)
    return val::UInt16
end

function cql_cluster_set_pending_requests_high_water_mark(cluster::Ptr{CassCluster}, siz::Int64)
    val = ccall(
        (:cass_cluster_set_pending_requests_high_water_mark, "libcassandra.so.2"),
        UInt16,
        (Ptr{CassCluster}, UInt32),
        cluster, siz)
    return val::UInt16
end

function cql_cluster_set_queue_size(cluster::Ptr{CassCluster}, siz::Int64)
    val1 = ccall(
        (:cass_cluster_set_queue_size_io, "libcassandra.so.2"),
        UInt16,
        (Ptr{CassCluster}, UInt32),
        cluster, siz)
    val2 = ccall(
        (:cass_cluster_set_queue_size_event, "libcassandra.so.2"),
        UInt16,
        (Ptr{CassCluster}, UInt32),
        cluster, siz)
    val = val1 | val2
    return val::UInt16
end

function cql_future_error_code(future::Ptr{CassFuture})
    val = ccall(
            (:cass_future_error_code, "libcassandra.so.2"),
            UInt16,
            (Ptr{CassFuture},),
            future)
    return val::UInt16
end

function cql_future_error_message(future::Ptr{CassFuture}, strref::Ref{Ptr{UInt8}}, siz::Ref{Csize_t})
    ccall(
        (:cass_future_error_message, "libcassandra.so.2"),
        Nothing,
        (Ptr{CassFuture}, Ref{Ptr{UInt8}}, Ref{Csize_t}),
        future, strref, siz)
end

function cql_cluster_new()
    val = ccall(
            (:cass_cluster_new, "libcassandra.so.2"),
            Ptr{CassCluster},
            ())
    return val::Ptr{CassCluster}
end

function cql_session_new()
    val = ccall(
            (:cass_session_new, "libcassandra.so.2"),
            Ptr{CassSession},
            ())
    return val::Ptr{CassSession}
end

function cql_cluster_set_credentials(cluster::Ptr{CassCluster}, username::String, password::String)
    ccall(
        (:cass_cluster_set_credentials, "libcassandra.so.2"),
        Nothing,
        (Ptr{CassCluster}, Cstring, Cstring),
        cluster, username, password)
end

function cql_cluster_set_contact_points(cluster::Ptr{CassCluster}, hosts::String)
    ccall(
        (:cass_cluster_set_contact_points, "libcassandra.so.2"),
        Nothing,
        (Ptr{CassCluster}, Cstring),
        cluster, hosts)
end

function cql_cluster_set_whitelist_filtering(cluster::Ptr{CassCluster}, hosts::String)
    ccall(
        (:cass_cluster_set_whitelist_filtering, "libcassandra.so.2"),
        Nothing,
        (Ptr{CassCluster}, Cstring),
        cluster, hosts)
end

function cql_cluster_set_blacklist_filtering(cluster::Ptr{CassCluster}, hosts::String)
    ccall(
        (:cass_cluster_set_blacklist_filtering, "libcassandra.so.2"),
        Nothing,
        (Ptr{CassCluster}, Cstring),
        cluster, hosts)
end

function cql_session_connect(session::Ptr{CassSession}, cluster::Ptr{CassCluster})
    val = ccall(
            (:cass_session_connect, "libcassandra.so.2"),
            Ptr{CassFuture},
            (Ptr{CassSession}, Ptr{CassCluster}),
            session, cluster)
    return val::Ptr{CassFuture}
end

function cql_session_free(session::Ptr{CassSession})
    ccall(
        (:cass_session_free, "libcassandra.so.2"),
        Nothing,
        (Ptr{CassCluster},),
        session)
end

function cql_cluster_free(cluster::Ptr{CassCluster})
    ccall(
        (:cass_cluster_free, "libcassandra.so.2"),
        Nothing,
        (Ptr{CassCluster},),
        cluster)
end

function cql_result_row_count(result::Ptr{CassResult})
    val = ccall(
            (:cass_result_row_count, "libcassandra.so.2"),
            Int32,
            (Ptr{CassResult},),
            result)
    return val::Int32
end

function cql_result_column_count(result::Ptr{CassResult})
    val = ccall(
            (:cass_result_column_count, "libcassandra.so.2"),
            Int32,
            (Ptr{CassResult},),
            result)
    return val::Int32
end

function cql_iterator_next(iterator::Ptr{CassIterator})
    next = ccall(
            (:cass_iterator_next, "libcassandra.so.2"),
            UInt8,
            (Ptr{CassIterator},),
            iterator)
    val = ifelse(next == 0, false, true)
    return val::Bool
end

function cql_future_free(future::Ptr{CassFuture})
    ccall(
        (:cass_future_free, "libcassandra.so.2"),
        Nothing,
        (Ptr{CassFuture},),
        future)
end

function cql_result_column_type(result::Ptr{CassResult}, idx::Int64)
    val = ccall(
            (:cass_result_column_type, "libcassandra.so.2"),
            UInt16,
            (Ptr{CassResult}, UInt32),
            result, idx)
    return val::UInt16
end

function cql_value_get_uuid(val::Ptr{CassValue}, out::Ref{CassUuid})
    err = ccall(
            (:cass_value_get_uuid, "libcassandra.so.2"),
            Cushort,
            (Ptr{CassValue}, Ref{CassUuid}),
            val, out)
    return err::UInt16
end

function cql_value_get_int8(val::Ptr{CassValue}, out::Ref{Cshort})
    err = ccall(
            (:cass_value_get_int8, "libcassandra.so.2"),
            Cushort,
            (Ptr{CassValue}, Ref{Cshort}),
            val, out)
    return err::UInt16
end

function cql_value_get_int16(val::Ptr{CassValue}, out::Ref{Cshort})
    err = ccall(
            (:cass_value_get_int16, "libcassandra.so.2"),
            Cushort,
            (Ptr{CassValue}, Ref{Cshort}),
            val, out)
    return err::UInt16
end

function cql_value_get_int64(val::Ptr{CassValue}, out::Ref{Clonglong})
    err = ccall(
            (:cass_value_get_int64, "libcassandra.so.2"),
            Cushort,
            (Ptr{CassValue}, Ref{Clonglong}),
            val, out)
    return err::UInt16
end

function cql_value_get_int32(val::Ptr{CassValue}, out::Ref{Cint})
    err = ccall(
            (:cass_value_get_int32, "libcassandra.so.2"),
            Cushort,
            (Ptr{CassValue}, Ref{Cint}),
            val, out)
    return err::UInt16
end

function cql_result_column_name(val::Ptr{CassResult}, pos::Int, out::Ref{Ptr{UInt8}}, siz::Ref{Csize_t})
    err = ccall(
            (:cass_result_column_name, "libcassandra.so.2"),
            Cushort,
            (Ptr{CassResult}, Clonglong, Ref{Ptr{UInt8}}, Ref{Csize_t}),
            val, pos, out, siz)
    return err::UInt16
end

function cql_value_get_uint32(val::Ptr{CassValue}, out::Ref{Cuint})
    err = ccall(
            (:cass_value_get_uint32, "libcassandra.so.2"),
            Cushort,
            (Ptr{CassValue}, Ref{Cuint}),
            val, out)
    return err::UInt16
end

function cql_value_get_bool(val::Ptr{CassValue}, out::Ref{Cint})
    err = ccall(
            (:cass_value_get_bool, "libcassandra.so.2"),
            Cushort,
            (Ptr{CassValue}, Ref{Cint}),
            val, out)
    return err::UInt16
end

function cql_value_get_string(val::Ptr{CassValue}, out::Ref{Ptr{UInt8}}, siz::Ref{Csize_t})
    err = ccall(
            (:cass_value_get_string, "libcassandra.so.2"),
            Cushort,
            (Ptr{CassValue}, Ref{Ptr{UInt8}}, Ref{Csize_t}),
            val, out, siz)
    return err::UInt16
end

function cql_value_get_bytes(val::Ptr{CassValue}, out::Ref{Ptr{UInt8}}, siz::Ref{Csize_t})
    err = ccall(
            (:cass_value_get_bytes, "libcassandra.so.2"),
            Cushort,
            (Ptr{CassValue}, Ref{Ptr{UInt8}}, Ref{Csize_t}),
            val, out, siz)
    return err::UInt16
end

function cql_value_get_float(val::Ptr{CassValue}, out::Ref{Cfloat})
    err = ccall(
            (:cass_value_get_float, "libcassandra.so.2"),
            Cushort,
            (Ptr{CassValue}, Ref{Cfloat}),
            val, out)
    return err::UInt16
end

function cql_value_get_double(val::Ptr{CassValue}, out::Ref{Cdouble})
    err = ccall(
            (:cass_value_get_double, "libcassandra.so.2"),
            Cushort,
            (Ptr{CassValue}, Ref{Cdouble}),
            val, out)
    return err::UInt16
end

function cql_statement_free(statement::Ptr{CassStatement})
    ccall(
        (:cass_statement_free, "libcassandra.so.2"),
        Nothing,
        (Ptr{CassStatement},),
        statement)
end

function cql_result_free(result::Ptr{CassResult})
    ccall(
        (:cass_result_free, "libcassandra.so.2"),
        Nothing,
        (Ptr{CassResult},),
        result)
end

function cql_iterator_free(iterator::Ptr{CassIterator})
    ccall(
        (:cass_iterator_free, "libcassandra.so.2"),
        Nothing,
        (Ptr{CassIterator},),
        iterator)
end

function cql_statement_new(query::String, params::Int)
    statement = ccall(
                    (:cass_statement_new, "libcassandra.so.2"),
                    Ptr{CassStatement},
                    (Cstring, Clonglong),
                    query, params)
    return statement::Ptr{CassStatement}
end

function cql_statement_set_paging_size(statement::Ptr{CassStatement}, pgsize::Int)
    ccall(
        (:cass_statement_set_paging_size, "libcassandra.so.2"),
        Nothing,
        (Ptr{CassStatement}, Cint),
        statement, pgsize)
end

function cql_statement_set_request_timeout(statement::Ptr{CassStatement}, timeout::Int)
    err = ccall(
            (:cass_statement_set_request_timeout, "libcassandra.so.2"),
            Cushort,
            (Ptr{CassStatement}, Clonglong),
            statement, timeout)
    return err::UInt16
end

function cql_session_execute(session::Ptr{CassSession}, statement::Ptr{CassStatement})
    future = ccall(
                (:cass_session_execute, "libcassandra.so.2"),
                Ptr{CassFuture},
                (Ptr{CassSession}, Ptr{CassStatement}),
                session, statement)
    return future::Ptr{CassFuture}
end

function cql_future_get_result(future::Ptr{CassFuture})
    result = ccall(
                (:cass_future_get_result, "libcassandra.so.2"),
                Ptr{CassResult},
                (Ptr{CassFuture},),
                future)
    return result::Ptr{CassResult}
end

function cql_iterator_from_result(result::Ptr{CassResult})
    iterator = ccall(
                (:cass_iterator_from_result, "libcassandra.so.2"),
                Ptr{CassIterator},
                (Ptr{CassResult},),
                result)
    return iterator::Ptr{CassIterator}
end

function cql_iterator_get_row(iterator::Ptr{CassIterator})
    row = ccall(
            (:cass_iterator_get_row, "libcassandra.so.2"),
            Ptr{CassRow},
            (Ptr{CassIterator},),
            iterator)
    return row::Ptr{CassRow}
end

function cql_row_get_column(row::Ptr{CassRow}, pos::Int64)
    val = ccall(
            (:cass_row_get_column, "libcassandra.so.2"),
            Ptr{CassValue},
            (Ptr{CassRow}, Clonglong),
            row, pos)
    return val::Ptr{CassValue}
end

function cql_result_has_more_pages(result::Ptr{CassResult})
    hasmore = ccall(
                (:cass_result_has_more_pages, "libcassandra.so.2"),
                Cint,
                (Ptr{CassResult},),
                result)
    out = convert(Bool, hasmore)
    return out::Bool
end

function cql_statement_set_paging_state(statement::Ptr{CassStatement}, result::Ptr{CassResult})
    ccall(
        (:cass_statement_set_paging_state, "libcassandra.so.2"),
        Nothing,
        (Ptr{CassStatement}, Ptr{CassResult}),
        statement, result)
end

function cql_future_wait(future::Ptr{CassFuture})
    ccall(
        (:cass_future_wait, "libcassandra.so.2"),
        Nothing,
        (Ptr{CassFuture},),
        future)
end

function cql_session_prepare(session::Ptr{CassSession}, query::String)
    future = ccall(
                (:cass_session_prepare, "libcassandra.so.2"),
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
                (:cass_batch_new, "libcassandra.so.2"),
                Ptr{CassBatch},
                (Cuchar,),
                batch_type)
    return batch::Ptr{CassBatch}
end

function cql_future_get_prepared(future::Ptr{CassFuture})
    prep = ccall(
            (:cass_future_get_prepared, "libcassandra.so.2"),
            Ptr{CassPrepared},
            (Ptr{CassFuture},),
            future)
    return prep::Ptr{CassPrepared}
end

function cql_prepared_bind(prep::Ptr{CassPrepared})
    statement = ccall(
                    (:cass_prepared_bind, "libcassandra.so.2"),
                    Ptr{CassStatement},
                    (Ptr{CassPrepared},),
                    prep)
    return statement::Ptr{CassStatement}
end

function cql_uuid_gen_new()
    uuid_gen = ccall(
        (:cass_uuid_gen_new, "libcassandra.so.2"),
        Ptr{CassUuidGen},
        ())
    return uuid_gen::Ptr{CassUuidGen}
end

function cql_uuid_gen_free(uuid_gen::Ptr{CassUuidGen})
    ccall(
        (:cass_uuid_gen_free, "libcassandra.so.2"),
        Nothing,
        (Ptr{CassUuidGen},),
        uuid_gen)
end

function cql_uuid_gen_random(uuid_gen::Ptr{CassUuidGen})
    uuid = get_null_cass_uuid_ref()
    ccall(
        (:cass_uuid_gen_random, "libcassandra.so.2"),
        Nothing,
        (Ptr{CassUuidGen}, Ref{CassUuid}),
        uuid_gen, uuid)
    return uuid.x
end

function cql_statement_bind_uuid(statement::Ptr{CassStatement}, pos::Int, data::UUID)
    ccall(
        (:cass_statement_bind_uuid, "libcassandra.so.2"),
        Nothing,
        (Ptr{CassStatement}, Cint, CassUuid),
        statement, pos, CassUuid(data))
end

function cql_statement_bind_string(statement::Ptr{CassStatement}, pos::Int, data::String)
    ccall(
        (:cass_statement_bind_string, "libcassandra.so.2"),
        Nothing,
        (Ptr{CassStatement}, Cint, Cstring),
        statement, pos, data)
end

function cql_statement_bind_bytes(statement::Ptr{CassStatement}, pos::Int, data::Vector{UInt8})
    ccall(
        (:cass_statement_bind_bytes, "libcassandra.so.2"),
        Nothing,
        (Ptr{CassStatement}, Cint, Ptr{UInt8}, Cint),
        statement, pos, data, length(data))
end

function cql_statement_bind_int8(statement::Ptr{CassStatement}, pos::Int, data::Int8)
    ccall(
        (:cass_statement_bind_int8, "libcassandra.so.2"),
        Nothing,
        (Ptr{CassStatement}, Cint, Cshort),
        statement, pos, data)
end

function cql_statement_bind_int16(statement::Ptr{CassStatement}, pos::Int, data::Int16)
    ccall(
        (:cass_statement_bind_int16, "libcassandra.so.2"),
        Nothing,
        (Ptr{CassStatement}, Cint, Cshort),
        statement, pos, data)
end

function cql_statement_bind_int32(statement::Ptr{CassStatement}, pos::Int, data::Int32)
    ccall(
        (:cass_statement_bind_int32, "libcassandra.so.2"),
        Nothing,
        (Ptr{CassStatement}, Cint, Cint),
        statement, pos, data)
end

function cql_statement_bind_int64(statement::Ptr{CassStatement}, pos::Int, data::Int64)
    ccall(
        (:cass_statement_bind_int64, "libcassandra.so.2"),
        Nothing,
        (Ptr{CassStatement}, Cint, Clonglong),
        statement, pos, data)
end

function cql_statement_bind_bool(statement::Ptr{CassStatement}, pos::Int, data::Bool)
    ccall(
        (:cass_statement_bind_bool, "libcassandra.so.2"),
        Nothing,
        (Ptr{CassStatement}, Cint, Cint),
        statement, pos, data)
end

function cql_statement_bind_uint32(statement::Ptr{CassStatement}, pos::Int, data::UInt32)
    ccall(
        (:cass_statement_bind_uint32, "libcassandra.so.2"),
        Nothing,
        (Ptr{CassStatement}, Cint, Cuint),
        statement, pos, data)
end

function cql_statement_bind_double(statement::Ptr{CassStatement}, pos::Int, data::Float64)
    ccall(
        (:cass_statement_bind_double, "libcassandra.so.2"),
        Nothing,
        (Ptr{CassStatement}, Cint, Cdouble),
        statement, pos, data)
end

function cql_statement_bind_float(statement::Ptr{CassStatement}, pos::Int, data::Float32)
    ccall(
        (:cass_statement_bind_float, "libcassandra.so.2"),
        Nothing,
        (Ptr{CassStatement}, Cint, Cfloat),
        statement, pos, data)
end

function cql_statement_bind_null(statement::Ptr{CassStatement}, pos::Int, ::Missing)
    ccall(
        (:cass_statement_bind_null, "libcassandra.so.2"),
        Nothing,
        (Ptr{CassStatement}, Cint),
        statement, pos)
end

function cql_batch_add_statement(batch::Ptr{CassBatch}, statement::Ptr{CassStatement})
    ccall(
        (:cass_batch_add_statement, "libcassandra.so.2"),
        Nothing,
        (Ptr{CassBatch}, Ptr{CassStatement}),
        batch, statement)
end

function cql_session_execute_batch(session::Ptr{CassSession}, batch::Ptr{CassBatch})
    future = ccall(
                (:cass_session_execute_batch, "libcassandra.so.2"),
                Ptr{CassFuture},
                (Ptr{CassSession}, Ptr{CassBatch}),
                session, batch)
    return future::Ptr{CassFuture}
end

function cql_batch_free(batch::Ptr{CassBatch})
    ccall(
        (:cass_batch_free, "libcassandra.so.2"),
        Nothing,
        (Ptr{CassBatch},),
        batch)
end

function cql_prepared_free(prep::Ptr{CassPrepared})
    ccall(
        (:cass_prepared_free, "libcassandra.so.2"),
        Nothing,
        (Ptr{CassPrepared},),
        prep)
end
