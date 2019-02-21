__precompile__(true)

module CQLdriver
using DataFrames, Dates, StructArrays, JuliaDB
export DataFrames, cqlinit, cqlclose, cqlwrite, cqlread, cqlexec

include("cqlwrapper.jl")
const CQL_OK = 0x0000

function Base.size(result::Ptr{CassResult})
    rows = cql_result_row_count(result)   
    cols = cql_result_column_count(result)
    return (Int(rows)::Int, Int(cols)::Int)
end

Base.size(data::IndexedTable) = length(data), length(colnames(data))

# Table helpers
cass_combine(tbl::AbstractDataFrame, other_tbl::AbstractDataFrame) = hcat(tbl, other_tbl)
cass_combine(tbl::IndexedTable, other_tbl::IndexedTable) = merge(tbl, other_tbl)

cass_tbl_select(tbl::IndexedTable, colindex::Int, rowindex::Int) = columns(tbl)[colindex][rowindex]
cass_tbl_select(tbl::AbstractDataFrame, colindex::Int, rowindex::Int) = tbl[rowindex, colindex]

cass_tbl_slice(tbl::IndexedTable, row_start::Int, row_end::Int) = view(tbl, row_start:row_end)
cass_tbl_slice(tbl::AbstractDataFrame, row_start::Int, row_end::Int) = view(tbl, row_start:row_end, :)

cass_col_names(tbl::IndexedTable) = colnames(tbl)
cass_col_names(tbl::AbstractDataFrame) = names(tbl)

cass_get_lastindex(tbl::IndexedTable) = lastindex(tbl)
cass_get_lastindex(tbl::AbstractDataFrame) = lastindex(tbl, 1)

"""
    function cqlinit(hosts; username, password, threads, connections, queuesize, bytelimit, requestlimit)
Change the performance characteristics of your CQL driver
# Arguments
- `hosts::String`: a string with ipv4 addreses of hosts 
- `username::String`: provide username for authenticated connections
- `password::String`: provide password for authenticated connections
- `threads::Int64`: set number of IO threads that handle query requests (default 1)
- `connections::Int64`: set number of connections per thread (default 2)
- `queuesize::Int64`: set queuesize that stores pending requests (default 4096)
- `bytelimit::Int64`: set max number of bytes pending on connection (default 65536 - 64KB)
- `requestlimit::Int64`: set max number of requests pending on connection (default 128 * connections)
- `whitelist::String`: set whiteslist of hosts - will only connect to these hosts and all other connections will be ignored
## Return
- `session::Ptr{CassSession}`: a pointer to the active session
- `cluster::Ptr{CassCluster}`: a pointer to the cluster
- `err::UInt`: a 16 bit integer with an error code. No error returns 0
"""
function cqlinit(hosts::String; username = "", password = "", whitelist = "", blacklist="", threads = 0, connections = 0, queuesize = 0, bytelimit = 0, requestlimit = 0)
    cluster = cql_cluster_new()
    session = cql_session_new()

    err = CQL_OK
    if username != ""
	cql_cluster_set_credentials(cluster, username, password)
    end
    if threads != 0
        err = cql_cluster_set_concurrency(cluster, threads) | err
    end
    if connections != 0
        err = cql_cluster_set_connections_per_host(cluster, connections) | err
    end
    if queuesize != 0
        err = cql_cluster_set_queue_size(cluster, queuesize) | err
    end
    if bytelimit != 0
        err = cql_cluster_set_write_bytes_high_water_mark(cluster, bytelimit) | err
    end
    if requestlimit != 0
        err = cql_cluster_set_pending_requests_high_water_mark(cluster, requestlimit) | err
    end
    if whitelist != ""
        cql_cluster_set_whitelist_filtering(cluster, whitelist)
    end
    if blacklist != ""
        cql_cluster_set_blacklist_filtering(cluster, blacklist)
    end

    cql_cluster_set_contact_points(cluster, hosts)
    future = cql_session_connect(session, cluster)
    err = cqlfuturecheck(future, "Session Connect") | err
    cql_future_free(future)    
    return session::Ptr{CassSession}, cluster::Ptr{CassCluster}, err::UInt16
end

"""
    function cql_future_check(future, caller)
Check if a future contains any errors
# Arguments
- `future::Ptr{CassFuture}`: a pointer to a future
- `caller::String`: a string to help identify where this function is called from
# Return
- `err::UInt`: a 16 bit integer with an error code. No error returns 0
"""
function cqlfuturecheck(future::Ptr{CassFuture}, caller::String = "")
    err = cql_future_error_code(future)
    # only prints valid messages for client errors
    if err != CQL_OK
        println("Error in CQL operation: ", caller)
        str = Vector{UInt8}(undef, 256)
        strref = Ref{Ptr{UInt8}}(pointer(str))
        siz = Ref{Csize_t}(sizeof(str))
        cql_future_error_message(future, strref, siz)
        println(unsafe_string(strref[], siz[]))
    end
    return err::UInt16
end

"""
    cql_val_type(result, idx)
Takes a CassResult and returns the type in a given column
# Arguments
- `result::Ptr{CassResult}`: a valid result from a query
- `idx::Int64`: the column to check
# Return
- `typ`: the type of the value in the specified column
"""
function cqlvaltype(result::Ptr{CassResult}, idx::Int64) 
# http://datastax.github.io/cpp-driver/api/cassandra.h/#enum-CassValueType
    val = cql_result_column_type(result, idx)
    val == 0x0009 ? typ = Int32    : # INTEGER   
    val == 0x0002 ? typ = Int64    : # BIGINT     
    val == 0x0005 ? typ = Int64    : # COUNTER   
    val == 0x0007 ? typ = Float64  : # DOUBLE    
    val == 0x0008 ? typ = Float32  : # FLOAT     
    val == 0x000A ? typ = String   : # TEXT      
    val == 0x000D ? typ = String      : # VARCHAR
    val == 0x000B ? typ = DateTime : # TIMESTAMP 
    val == 0x0014 ? typ = Int8     : # TINYINT
    val == 0x0013 ? typ = Int16    : # SMALLINT
    val == 0x0011 ? typ = Date     : # DATE      
    val == 0x0004 ? typ = Bool     : # BOOLEAN   
    val == 0x000C ? typ = UInt128  : # UUID
    val == 0x000F ? typ = UInt128  : # TIMEUUID
    val == 0x000E ? typ = BigInt   : # VARINT
    val == 0x0010 ? typ = IPAddr   : # INET 
    val == 0x0006 ? typ = BigFloat : # DECIMAL
    val == 0x0012 ? typ = Missing      : # TIME
    val == 0x0001 ? typ = Missing      : # ASCII
    val == 0x0003 ? typ = Missing      : # BLOB
    val == 0xFFFF ? typ = Missing      : # UNKNOWN
    val == 0x0000 ? typ = Missing      : # CUSTOM
    val == 0x0015 ? typ = Missing      : # DURATION
    val == 0x0020 ? typ = Missing      : # LIST
    val == 0x0021 ? typ = Missing      : # MAP
    val == 0x0022 ? typ = Missing      : # SET
    val == 0x0030 ? typ = Missing      : # UDT
    val == 0x0031 ? typ = Missing      : # TUPLE
    typ = Missing
    if typ == Missing 
        typ = UInt8 
        println("Warning, unsupported datatype: $(num2hex(val))
        https://docs.datastax.com/en/developer/cpp-driver/2.8/api/cassandra.h/#enum-CassValueType")
    end
    return Union{typ, Missing}
end

"""
    function cqlgetvalue(val, t, strlen)
retrieve value using the correct type
# Arguments
- `val::Ptr{CassValue}`: a returned value from a query
- `t::DataType`: the type of the value being extracted
- `strlen::Int`: for string values specify max-length of output
# Return
- `out`: the return value, can by of any type
"""
function cqlgetvalue(val::Ptr{CassValue}, T::Type{Union{Int64, Missing}}, strlen::Int)
    num = Ref{Clonglong}(0)
    err = cql_value_get_int64(val, num)
    return ifelse(err == CQL_OK, num[], missing)
end

function cqlgetvalue(val::Ptr{CassValue}, T::Type{Union{Bool, Missing}}, strlen::Int)
    num = Ref{Cint}(0)
    err = cql_value_get_bool(val, num)
    return ifelse(err == CQL_OK, Bool(num[]), missing)
end

function cqlgetvalue(val::Ptr{CassValue}, T::Type{Union{Int32, Missing}}, strlen::Int)
    num = Ref{Cint}(0)
    err = cql_value_get_int32(val, num)
    return ifelse(err == CQL_OK, num[], missing)
end

function cqlgetvalue(val::Ptr{CassValue}, T::Type{Union{Int16, Missing}}, strlen::Int)
    num = Ref{Cshort}(0)
    err = cql_value_get_int16(val, num)
    return ifelse(err == CQL_OK, num[], missing)
end

function cqlgetvalue(val::Ptr{CassValue}, T::Type{Union{Int8, Missing}}, strlen::Int)
    num = Ref{Cshort}(0)
    err = cql_value_get_int8(val, num)
    return ifelse(err == CQL_OK, num[], missing)
end

function cqlgetvalue(val::Ptr{CassValue}, T::Type{Union{String, Missing}}, strlen::Int)
    str = Vector{UInt8}(undef, strlen)
    strref = Ref{Ptr{UInt8}}(pointer(str))
    siz = Ref{Csize_t}(sizeof(str))
    err = cql_value_get_string(val, strref, siz)
    return ifelse(err == CQL_OK, unsafe_string(strref[], siz[]), missing)
end

function cqlgetvalue(val::Ptr{CassValue}, T::Type{Union{Float64, Missing}}, strlen::Int)
    num = Ref{Cdouble}(0)
    err = cql_value_get_double(val, num)
    return ifelse(err == CQL_OK, num[], missing)
end

function cqlgetvalue(val::Ptr{CassValue}, T::Type{Union{Float32, Missing}}, strlen::Int)
    num = Ref{Cfloat}(0)
    err = cql_value_get_float(val, num)
    return ifelse(err == CQL_OK, num[], missing)
end

function cqlgetvalue(val::Ptr{CassValue}, T::Type{Union{Date, Missing}}, strlen::Int)
    num = Ref{Cuint}(0)
    err = cql_value_get_uint32(val, num)
    s = string(num[])
    l = length(s)
    o = ifelse(l == 8, s[1:4]*"-"*s[5:6]*"-"*s[7:8], "")
    return ifelse(err == CQL_OK, Date(o), missing)
end

function cqlgetvalue(val::Ptr{CassValue}, T::Type{Union{DateTime, Missing}}, strlen::Int)
    unixtime = Ref{Clonglong}(0)
    err = cql_value_get_int64(val, unixtime)
    return ifelse(err == CQL_OK, Dates.unix2datetime(unixtime[]/1000), missing)
end

cqlgetvalue(val::Ptr{CassValue}, T::Type{Union}, strlen::Int) = missing

"""
    function cqlstrprep(table, data)
create a prepared query string for use with batch inserts
# Arguments
- `table::String`: name of the table on the server
- `columns::Array{String}`: name of the columns on the server
- `data::Array{Any,1}`: an array of data to be inserted
# Return
- `out::String`: a valid INSERT or UPDATE query
"""
function cqlstrprep(table::String, data::Union{IndexedTable, AbstractDataFrame}; update::Union{IndexedTable, AbstractDataFrame, Nothing}=nothing, counter::Bool=false)
    out = ""
    if update == nothing
        datacolnames = string.(cass_col_names(data))
        cols, vals = "", ""

        for c in datacolnames
            cols = cols * c * ","
            vals = vals * "?,"
        end
        out = "INSERT INTO " * table * " (" * cols[1:end-1] * ") VALUES (" * vals[1:end-1] * ")"
    else write == :update
        datacolnames = string.(cass_col_names(data))
        updtcolnames = string.(cass_col_names(update))
        cols, vals = "", ""
        for c in datacolnames
            
            cols = cols * c * "=" * ifelse(counter, c*"+?, ", "?, ")
        end
        for u in updtcolnames
            vals = vals * u * "=? AND "
        end
        out = "UPDATE " * table * " SET " * cols[1:end-2] * " WHERE " * vals[1:end-5]
    end
    return out::String
end

"""
    function cqlstatementbind(statement, pos, typ, data)
Bind data to a column in a statement for use with batch inserts
# Arguments
- `statement::Ptr{CassStatement}`: pointer to a statement
- `pos::Int`: what column to put data into
- `typ::DataType, data)`: the datatype of the data
# Return
- `Void`:
"""
cqlstatementbind(statement::Ptr{CassStatement}, pos::Int, data::Missing) = nothing # default to unset_value
cqlstatementbind(statement::Ptr{CassStatement}, pos::Int, data::String) = cql_statement_bind_string(statement, pos, data)
cqlstatementbind(statement::Ptr{CassStatement}, pos::Int, data::Bool) = cql_statement_bind_bool(statement, pos, data)
cqlstatementbind(statement::Ptr{CassStatement}, pos::Int, data::Int8) = cql_statement_bind_int8(statement, pos, data)
cqlstatementbind(statement::Ptr{CassStatement}, pos::Int, data::Int16) = cql_statement_bind_int16(statement, pos, data)
cqlstatementbind(statement::Ptr{CassStatement}, pos::Int, data::Int32) = cql_statement_bind_int32(statement, pos, data)
cqlstatementbind(statement::Ptr{CassStatement}, pos::Int, data::Int64) = cql_statement_bind_int64(statement, pos, data)
cqlstatementbind(statement::Ptr{CassStatement}, pos::Int, data::Float32) = cql_statement_bind_float(statement, pos, data)
cqlstatementbind(statement::Ptr{CassStatement}, pos::Int, data::Float64) = cql_statement_bind_double(statement, pos, data)

function cqlstatementbind(statement::Ptr{CassStatement}, pos::Int, data::Date)
    d = parse(UInt32, replace(string(data),"-" => ""))
    cql_statement_bind_uint32(statement, pos, d)
end

function cqlstatementbind(statement::Ptr{CassStatement}, pos::Int, data::DateTime)
    d = convert(Int64, Dates.datetime2unix(data)*1000)
    cql_statement_bind_int64(statement, pos, d)
end

"""
function cqlclose(session, cluster)
Decommission a connection and free its resources
# Arguments
- `session::Ptr{CassSession}`: the current active session
- `cluster::Ptr{CassCluster}`: the cluster associated with the active session
# Return
- `Void`:
"""
function cqlclose(session::Ptr{CassSession}, cluster::Ptr{CassCluster})
    cql_session_free(session)
    cql_cluster_free(cluster)
end

function _cqlresultscheck(session::Ptr{CassSession}, statement::Ptr{CassStatement}, retries::Int)
    future = nothing
    while(true)
        future = cql_session_execute(session, statement)
        err = cqlfuturecheck(future, "Session Execute")
        err == CQL_OK && break
        if (err != CQL_OK) & (retries == 0)
            cql_statement_free(statement)
            cql_future_free(future)
            return err
        end
        sleep(1)
        retries -= 1
        cql_future_free(future)
    end
    return CQL_OK, future
end

function _cqlprocessresult!(result::Ptr{CassResult}, output_arr::StructArray{NT}, statement::Ptr{CassStatement}, types::Vector, cols::Int, strlen::Int) where {NT}
    iterator = cql_iterator_from_result(result)
    @inbounds for r = eachindex(output_arr)
        cql_iterator_next(iterator)
        row = cql_iterator_get_row(iterator)
        output_arr[r] = NT(Tuple([cqlgetvalue(cql_row_get_column(row, c-1), types[c], strlen) for c = 1:cols]))
    end
    morepages = cql_result_has_more_pages(result)
    cql_statement_set_paging_state(statement, result)
    cql_iterator_free(iterator)
    cql_result_free(result)

    return morepages
end

"""
    function cqlread(session, query; pgsize, retries, strlen)
    function cqlread(session, queries, concurrency; strlen)
Query the server for the contents of a table
- `session::Ptr{CassSession}`: pointer to the active session
- `query::String`: a valid SELECT query
- `queries::Array{String}`: an array of valid queries
- `concurrency::Int=500`: how many queries to execute 
- `pgsize::Int=10000`: how many lines to pull at a time
- `retries::Int=5`: number of times to retry pulling a page of data
- `timeout::Int=10000`: time to wait for response in milliseconds
- `strlen::Int=128`: the maximum number of characters in a string
# Return
- `err::UInt16`: status of the query
- `output::DataFrame`: a dataframe with named columns
- `outputs::Array{DataFrame}`: an array of dataframe results
"""
function cqlread(session::Ptr{CassSession}, query::String; pgsize::Int=10000, retries::Int=5, timeout::Int=10000, strlen::Int=128)
    statement = cql_statement_new(query, 0)
    cql_statement_set_request_timeout(statement, timeout)
    cql_statement_set_paging_size(statement, pgsize)
    
    # output = DataFrame()
    morepages = true
    err = CQL_OK

    # process first page
    err, future = _cqlresultscheck(session, statement, retries)
    if err != CQL_OK
        return err::UInt16, StructArray()
    end

    # get result
    result = cql_future_get_result(future)
    cql_future_free(future)
    rows, cols = size(result)

    # define all the types we will need
    types = [cqlvaltype(result, c-1) for c = 1:cols]
    names = Array{Symbol}(UndefInitializer(), cols)
    
    @inbounds for c = eachindex(names)
        str = zeros(UInt8, strlen)
        strref = Ref{Ptr{UInt8}}(pointer(str))
        siz = Ref{Csize_t}(sizeof(str))
        errcol = cql_result_column_name(result, c-1, strref, siz)
        names[c] = Symbol(ifelse(errcol == CQL_OK, unsafe_string(strref[], siz[]), string("C",c)))
    end
    NT = NamedTuple{Tuple(names), Tuple{types...}}
    SA_Type = StructArray{NT}
    output = SA_Type(undef, rows)

    morepages = _cqlprocessresult!(result, output, statement, types, cols, strlen)

    while(morepages)
        err, future = _cqlresultscheck(session, statement, retries)
        if err != CQL_OK
            return err::UInt16, output::SA_Type
        end
    
        # get result
        result = cql_future_get_result(future)
        cql_future_free(future)
        rows, cols = size(result)
        output_arr = SA_Type(undef, rows)

        morepages = _cqlprocessresult!(result, output_arr, statement, types, cols, strlen)

        output = vcat(output, output_arr)
    end
    cql_statement_free(statement)
    return err::UInt16, output::SA_Type
end

function cqlread(session::Ptr{CassSession}, queries::Array{String}; concurrency::Int=500, retries::Int=5, timeout::Int=10000, strlen::Int=128)
    out = Array{StructArray}(UndefInitializer(), 0)
    err = CQL_OK

    for query in 1:concurrency:length(queries)
        concurrency = ifelse(length(queries)-query < concurrency, length(queries)-query+1, concurrency)

        futures = Array{Ptr{CassFuture}}(UndefInitializer(), 0)        
        for c in 1:concurrency
            statement = cql_statement_new(queries[query+c-1], 0)
            cql_statement_set_request_timeout(statement, timeout)
            push!(futures, cql_session_execute(session, statement))
            cql_statement_free(statement)
        end

        results = Array{Ptr{CassResult}}(UndefInitializer(), 0)
        for f in 1:length(futures)
            retry = retries
            future = futures[f]
            while(true)
                futerr = cqlfuturecheck(future, "Async Read")
                if futerr == CQL_OK 
                    push!(results, cql_future_get_result(future))
                    cql_future_free(future)
                    break
                end
                if (futerr != CQL_OK) & (retry == 0)
                    err = futerr
                    cql_future_free(future)
                    break
                end
                sleep(1)
                retry -= 1
                statement = cql_statement_new(queries[query+f-1], 0)
                cql_statement_set_request_timeout(statement, timeout)
                future = cql_session_execute(session, statement)
                cql_statement_free(statement)
            end
        end

        for result in results
            rows, cols = size(result)
            iterator = cql_iterator_from_result(result)
            sa, types = cqlbuildstructarray(result, strlen)
            NT = eltype(sa)
            @inbounds for r = eachindex(sa)
                cql_iterator_next(iterator)
                row = cql_iterator_get_row(iterator)
                sa[r] = NT(Tuple([cqlgetvalue(cql_row_get_column(row, c-1), types[c], strlen) for c = 1:cols]))
            end
            push!(out, sa)
            cql_iterator_free(iterator)
            cql_result_free(result)            
        end
    end
    return err::UInt16, out
end

function cqlbuilddf(result::Ptr{CassResult}, strlen::Int)
    rows, cols = size(result)
    types = Array{Union}(UndefInitializer(), cols)
    for c in 1:cols
        types[c] = cqlvaltype(result, c-1)
    end
    names = Array{Symbol}(UndefInitializer(), cols)
    for c in 1:cols
        str = zeros(UInt8, strlen)
        strref = Ref{Ptr{UInt8}}(pointer(str))
        siz = Ref{Csize_t}(sizeof(str))
        errcol = cql_result_column_name(result, c-1, strref, siz)
        names[c] = Symbol(ifelse(errcol == CQL_OK, unsafe_string(strref[], siz[]), string("C",c)))
    end
    output = DataFrame(types, names, 0)
    return output::DataFrame, types::Array{Union}
end

function cqlbuildstructarray(result::Ptr{CassResult}, strlen::Int)
    rows, cols = size(result)
    types = [cqlvaltype(result, c-1) for c = 1:cols]
    names = Array{Symbol}(UndefInitializer(), cols)
    for c in 1:cols
        str = zeros(UInt8, strlen)
        strref = Ref{Ptr{UInt8}}(pointer(str))
        siz = Ref{Csize_t}(sizeof(str))
        errcol = cql_result_column_name(result, c-1, strref, siz)
        names[c] = Symbol(ifelse(errcol == CQL_OK, unsafe_string(strref[], siz[]), string("C",c)))
    end
    NT = NamedTuple{Tuple(names), Tuple{types...}}
    SA_Type = StructArray{NT}
    output = SA_Type(undef, rows)
    return output::SA_Type, types
end

"""
    function cqlbatchwrite(session, table, data; retries, update, counter)
Write a set of rows to a table as a prepared batch
# Arguments
- `session::Ptr{CassSession}`: pointer to the active session
- `table::String`: the name of the table you want to write to
- `data::DataFrame`: a DataFrame with named columns
- `retries::Int=5`: number of retries per batch insert
- `update::DataFrame`: the arguments for WHERE during an UPDATE
- `counter::Bool`: for updating the counter datatype
# Return
- `err::UInt16`: status of the batch insert
"""
function cqlbatchwrite(session::Ptr{CassSession}, cass_table::String, data::AbstractDataFrame; retries::Int=5, update::Union{AbstractDataFrame, Nothing}=nothing, counter::Bool=false)
    query = cqlstrprep(cass_table, data, update=update, counter=counter)
    future = cql_session_prepare(session, query)
    cql_future_wait(future)
    err = cqlfuturecheck(future, "Session Prepare") 
    if err != CQL_OK 
        cql_future_free(future)
        return err::UInt16
    end
    
    prep = cql_future_get_prepared(future)
    cql_future_free(future)
    batchtype = ifelse(!counter, 0x00, 0x02)
    batch = cql_batch_new(batchtype)
    rows, cols = size(data)
    frame = data
    if update != nothing
        urows, ucols = size(update)
        cols += ucols
        frame = hcat(data, update)
    end
    for r in 1:rows
        statement = cql_prepared_bind(prep)
        for c in 1:cols
            cqlstatementbind(statement, c-1, frame[r,c])
        end
        cql_batch_add_statement(batch, statement)
        cql_statement_free(statement)
    end
    while(true)
        future = cql_session_execute_batch(session, batch)
        cql_future_wait(future)
        err = cqlfuturecheck(future, "Execute Batch")
        cql_future_free(future)
        err == CQL_OK && break
        retries == 0 && break
        retries -= 1
        sleep(1)
    end
    cql_prepared_free(prep)
    cql_batch_free(batch)
    return err::UInt16
end

function cqlbatchwrite(session::Ptr{CassSession}, table::String, data::IndexedTable; retries::Int=5, update::Union{IndexedTable, Nothing}=nothing, counter::Bool=false)
    query = cqlstrprep(table, data, update=update, counter=counter)
    future = cql_session_prepare(session, query)
    cql_future_wait(future)
    err = cqlfuturecheck(future, "Session Prepare") 
    if err != CQL_OK 
        cql_future_free(future)
        return err::UInt16
    end
    
    prep = cql_future_get_prepared(future)
    cql_future_free(future)
    batchtype = ifelse(!counter, 0x00, 0x02)
    batch = cql_batch_new(batchtype)
    rows, cols = size(data)
    frame = data
    if update != nothing
        urows, ucols = size(update)
        cols += ucols
        frame = merge(data, update)
    end
    for r in 1:rows
        statement = cql_prepared_bind(prep)
        for c in 1:cols
            cqlstatementbind(statement, c-1, columns(frame)[c][r])
        end
        cql_batch_add_statement(batch, statement)
        cql_statement_free(statement)
    end
    while(true)
        future = cql_session_execute_batch(session, batch)
        cql_future_wait(future)
        err = cqlfuturecheck(future, "Execute Batch")
        cql_future_free(future)
        err == CQL_OK && break
        retries == 0 && break
        retries -= 1
        sleep(1)
    end
    cql_prepared_free(prep)
    cql_batch_free(batch)
    return err::UInt16
end

"""
    function cqlrowwrite(session, table, data; retries, update, counter)
Write one row of data to a table
# Arguments
- `session::Ptr{CassSession}`: pointer to the active session
- `cass_table::String`: the name of the table you want to write to
- `data::Union{IndexedTable, DataFrame}`: a DataFrame or IndexedTable with named columns
- `retries::Int=5`: number of retries per batch insert
- `update::DataFrame`: the arguments for WHERE during an UPDATE
- `counter::Bool`: for updating the counter datatype
# Return
- `err::UInt16`: status of the insert
"""

function cqlrowwrite(session::Ptr{CassSession}, cass_table::String, data::Union{DataFrame, IndexedTable}; retries::Int=5, update::Union{DataFrame, IndexedTable, Nothing} = nothing, counter::Bool = false)
    err = CQL_OK
    query = cqlstrprep(cass_table, data, update=update, counter=counter)
    rows, cols = size(data)
    frame = data
    if update != nothing
        urows, ucols = size(update)
        cols += ucols
        frame = cass_combine(data, update)
    end
    statement = cql_statement_new(query, cols)
    for c in 1:cols
        cqlstatementbind(statement, c-1, cass_tbl_select(frame, c, 1))
    end

    while(true) 
        future = cql_session_execute(session, statement)
        cql_future_wait(future)
        err = cqlfuturecheck(future, "Execute Statement")
        cql_future_free(future)
        err == CQL_OK && break
        retries == 0 && break
        retries -= 1
        sleep(1)
    end
    cql_statement_free(statement)
    return err::UInt16
end

"""
    function cqlwrite(session, table, data; batchsize, retries, update, counter)
Write to a table
# Arguments
- `session::Ptr{CassSession}`: pointer to the active session
- `table::String`: the name of the table you want to write to
- `data::DataFrame`: a DataFrame with named columns
- `retries::Int=5`: number of retries per batch insert
- `batchsize::Int=1000`: number of rows to write per batch
- `update::DataFrame`: the arguments for WHERE during an UPDATE
- `counter::Bool`: for updating the counter datatype
# Return
- `err::UInt16`: status of the insert
"""
function cqlwrite(s::Ptr{CassSession}, cass_table::String, data::Union{DataFrame, IndexedTable}; update::Union{DataFrame, IndexedTable, Nothing}=nothing, batchsize::Int=500, retries::Int=5, counter::Bool=false, returnanyerror=false) 
    rows, cols = size(data)
    rows == 0 && return 0x9999
    err = CQL_OK
    if rows == 1
        err = cqlrowwrite(s, cass_table, data, retries=retries, update=update, counter=counter)
    elseif rows <= batchsize
        err = cqlbatchwrite(s, cass_table, data, retries=retries, update=update, counter=counter)
    else
        pages = (rows ÷ batchsize)
        errs = zeros(UInt16, pages)
        @sync for p in 1:pages
            to = p * batchsize
            fr = to - batchsize + 1
            if p < pages                
                @async errs[p] = cqlbatchwrite(s, cass_table, cass_tbl_slice(data, fr, to), retries=retries, update=update == nothing ? nothing : cass_tbl_slice(update, fr, to), counter=counter)
            else
                @async errs[p] = cqlbatchwrite(s, cass_table, cass_tbl_slice(data, fr, cass_get_lastindex(data)), retries=retries, update=update==nothing ? nothing : cass_tbl_slice(update, fr, cass_get_lastindex(update)), counter=counter)
            end
        end
        reterr = union(errs)
        if returnanyerror
            err = reterr[end]
        else
            err = reterr[1]
        end
    end
    return err::UInt16
end

function cqlwrite(s::Ptr{CassSession}, cass_table::String, data::JuliaDB.DIndexedTable; paritionsize::Int=100000, batchsize::Int=500, retries::Int=5, counter::Bool=false)
    errs = Vector{UInt16}()
    for tbl = Iterators.partition(data, paritionsize)
        push!(errs, cqlwrite(s, cass_table, tbl; batchsize=batchsize, retries=retries, counter=counter))
    end
    return union(errs)[1]
end

"""
    function cqlexec(session, statement)
Execute arbitrary command to the CQL database
# Arguments
- `session::Ptr{CassSession}`: pointer to the active session
- `statement::String`: a valid CQL command
# Return
- `err::UInt16`: status of the command
"""
function cqlexec(session::Ptr{CassSession}, cmd::String)
    err = CQL_OK
    statement = cql_statement_new(cmd, 0)
    future = cql_session_execute(session, statement)
    cql_future_wait(future)
    err = cqlfuturecheck(future, "Execute Statement")
    cql_future_free(future)
    cql_statement_free(statement)
    return err::UInt16
end
   
end
