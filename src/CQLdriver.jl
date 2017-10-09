module CQLdriver

export cqlinit, cqlclose, cqlbatchwrite, cqlasyncwrite, cqlread

include("cqlwrapper.jl")
const CQL_OK = 0

function Base.size(result::Ptr{CassResult})
    rows = cql_result_row_count(result)   
    cols = cql_result_column_count(result)
    return (Int(rows)::Int, Int(cols)::Int)
end

"""
    function cql_future_check(future::Ptr{CassFuture}, caller::String = "")
Check if a future contains any errors
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
        str = zeros(Vector{UInt8}(256))
        strref = Ref{Ptr{UInt8}}(pointer(str))
        siz = pointer_from_objref(sizeof(str))
        cql_future_error_message(future, strref, siz)
        println(unsafe_string(strref[]))
    end
    return err::UInt16
end

"""
    function cql_val_type(result::Ptr{CassResult}, val::Int64)
Decommission a connection and free its resources
- `result::Ptr{CassResult}`: a valid result from a query
- `idx::Int64`: the column to check
# Return
- `typ::DataType`: the type of the value in the specified column
"""
function cqlvaltype(result::Ptr{CassResult}, idx::Int64) 
# http://datastax.github.io/cpp-driver/api/cassandra.h/#enum-CassValueType
    val = cql_result_column_type(result, idx)
    val == 0x0001 ? typ = String   : # ASCII
    val == 0x000A ? typ = String   : # TEXT
    val == 0x0010 ? typ = String   : # INET
    val == 0x0011 ? typ = String   : # DATE
    val == 0x0012 ? typ = String   : # TIME
    val == 0x000D ? typ = String   : # VARCHAR
    val == 0x0014 ? typ = UInt8    : # TINYINT
    val == 0x0013 ? typ = UInt16   : # SMALLINT
    val == 0x000C ? typ = UInt128  : # UUID
    val == 0x000F ? typ = UInt128  : # TIMEUUID
    val == 0x0009 ? typ = Int32    : # INTEGER
    val == 0x0002 ? typ = Int64    : # BIGINT
    val == 0x0005 ? typ = Int64    : # COUNTER
    val == 0x000E ? typ = BigInt   : # VARINT
    val == 0x0004 ? typ = Bool     : # BOOLEAN
    val == 0x0007 ? typ = Float32  : # DOUBLE
    val == 0x0008 ? typ = Float64  : # FLOAT
    val == 0x0006 ? typ = BigFloat : # DECIMAL
    val == 0x000B ? typ = DateTime : # TIMESTAMP
    val == 0x0003 ? typ = Any      : # BLOB
    val == 0xFFFF ? typ = Any      : # UNKNOWN
    val == 0x0000 ? typ = Any      : # CUSTOM
    val == 0x0015 ? typ = Any      : # DURATION
    val == 0x0020 ? typ = Any      : # LIST
    val == 0x0021 ? typ = Any      : # MAP
    val == 0x0022 ? typ = Any      : # SET
    val == 0x0030 ? typ = Any      : # UDT
    val == 0x0031 ? typ = Any      : # TUPLE
    typ = Any
    return typ::DataType
end

"""
    function cqlgetvalue(val::Ptr{CassValue}, t::DataType, strlen::Int)
retrieve value using the correct type
- `val::Ptr{CassValue}`: a returned value from a query
- `t::DataType`: the type of the value being extracted
- `strlen::Int`: for string values specify max-length of output
# Return
- `out::T`: the return value, can by of any type
"""
function cqlgetvalue(val::Ptr{CassValue}, T::DataType, strlen::Int)
    if T == Int64
        num = Ref{Clonglong}(0)
        err = cql_value_get_int64(val, num)
        out = ifelse(err == CQL_OK, num[], Int64(0))
    elseif T == Int32
        num = Ref{Cint}(0)
        err = cql_value_get_int32(val, num)
        out = ifelse(err == CQL_OK, num[], Int32(0))
    elseif T == String
        str = zeros(Vector{UInt8}(strlen))
        strref = Ref{Ptr{UInt8}}(pointer(str))
        siz = pointer_from_objref(sizeof(str))
        err = cql_value_get_string(val, strref, siz)
        out = ifelse(err == CQL_OK, unsafe_string(strref[]), "")
    elseif T == Float64
        num = Ref{Cdouble}(0)
        err = cql_value_get_float(val, num)
        out = ifelse(err == CQL_OK, num[], 0.0)
    elseif T == DateTime
        unixtime = Ref{Clonglong}(0)
        err = cql_value_get_int64(val, unixtime)
        time = ifelse(err == CQL_OK, unixtime[]/1000, 0)
        out = Dates.unix2datetime(time)
    end
    return out::T
end

"""
    function cqlstrprep(table::String, columns::Array{String}, data::Array{Any,1})
create a prepared query string for use with batch inserts
- `table::String`: name of the table on the server
- `columns::Array{String}`: name of the columns on the server
- `data::Array{Any,1}`: an array of data to be inserted
# Return
- `out::String`: a valid INSERT query
"""
function cqlstrprep(table::String, columns::Array{String}, data::Array{Any,1})
    cols = ""
    vals = ""
    for c in columns
        cols = cols * c * ","
        vals = vals * "?,"
    end
    out = "INSERT INTO "* table *" ("* cols[1:end-1] *") VALUES ("* vals[1:end-1] *")"
    return out::String
end

"""
    function cqlstatementbind(statement::Ptr{CassStatement}, pos::Int, typ::DataType, data::T)
bind data to a column in a statement for use with batch inserts
- `statement::Ptr{CassStatement}`: pointer to a statement
- `pos::Int`: what column to put data into
- `typ::DataType, data)`: the datatype of the data
# Return
- `Void`:
"""
function cqlstatementbind(statement::Ptr{CassStatement}, pos::Int, typ::DataType, data)
    if typ == String
        cql_statement_bind_string(statement, pos, data)
    elseif typ == Int32
        cql_statement_bind_int32(statement, pos, data)
    elseif typ == Int64
        cql_statement_bind_int64(statement, pos, data)
    elseif typ == Float64
        cql_statement_bind_double(statement, pos, data)
    elseif typ == DateTime
        d = convert(Int64, Dates.datetime2unix(data)*1000)
        cql_statement_bind_int64(statement, pos, d)
    end
end



"""
function cqlinit(hosts::String)    
Establish a new connection to a cluster
- `hosts::String`: a string of comma separated IP addresses
# Return
- `session::Ptr{CassSession}`: a pointer to the active session
- `cluster::Ptr{CassCluster}: a pointer to the active cluster`
- `err::UInt`: a 16 bit integer with an error code. No error returns 0
"""
function cqlinit(hosts::String)
    cluster = cql_cluster_new()
    session = cql_session_new()
    cql_cluster_set_contact_points(cluster, hosts)
    future = cql_session_connect(session, cluster)
    err = cqlfuturecheck(future, "Session Connect")
    cql_future_free(future)    
    return session::Ptr{CassSession}, cluster::Ptr{CassCluster}, err::UInt16
end

"""
function cqlclose(session::Ptr{CassSession}, cluster::Ptr{CassCluster})
Decommission a connection and free its resources
- `session::Ptr{CassSession}`: the current active session
- `cluster::Ptr{CassCluster}`: the cluster associated with the active session
# Return
- `Void`:
"""
function cqlclose(session::Ptr{CassSession}, cluster::Ptr{CassCluster})
    cql_session_free(session)
    cql_cluster_free(cluster)
end

"""
    function cqlread(session::Ptr{CassSession}, query::String, pgsize=10000, strlen=128)
query the server for the contents of a table
- `session::Ptr{CassSession}`: pointer to the active session
- `query::String`: a valid SELECT query
- `pgsize=10000`: how many lines to pull at a time
- `strlen=128`: the maximum size of string columns
# Return
- `err::UInt`: status of the query
- `finalarray::Array{Any, 2}`: a 2 dimensional array containing the results
"""
function cqlread(session::Ptr{CassSession}, query::String, pgsize=10000, strlen=128)
    statement = cql_statement_new(query)
    cql_statement_set_paging_size(statement, pgsize)
        
    finalarray = [[]]
    morepages = true
    while(morepages)    
        future = cql_session_execute(session, statement)
        err = cqlfuturecheck(future, "Session Execute") 
        if err != CQL_OK
            cql_statement_free(statement)
            cql_future_free(future)
            return err, finalarray
        end
        result = cql_future_get_result(future)

        # check dimensions of the result
        # remember the types in each column
        dim = size(result)
        types = Array{DataType, 2}(1, dim[2])
        for i in 1:dim[2]
            types[i] = cqlvaltype(result, i-1)
        end

        # fill an array with arrays of values of varying types
        # e.g. [[1, "hi", 3.0],
        #       [2, "ho", 2.1]]
        iterator = cql_iterator_from_result(result)
        output = Array{Any, 2}(dim)
        for j in 1:dim[1]
            cql_iterator_next(iterator)
            row = cql_iterator_get_row(iterator)
            for i in 1:dim[2]
                val = cql_row_get_column(row, i-1)
                output[j,i] = cqlgetvalue(val, types[i], strlen)
            end      
        end
        finalarray = ifelse(length(finalarray) == 0, output, vcat(finalarray, output))
        
        # check if there are more pages of values in the result
        morepages = cql_result_has_more_pages(result)
        cql_statement_set_paging_state(statement, result)
        cql_iterator_free(iterator)
        cql_result_free(result)
    end
    cql_statement_free(statement)
    return CQL_OK, finalarray
end

"""
    function cqlbatchwrite(session::Ptr{CassSession}, table::String, columns::Array{String}, data::Array{Any,1})
batch insert data arrays
- `session::Ptr{CassSession}`: pointer to the active session
- `table::String`: the name of the table you want to write to
- `columns::Array{String}`: an array of existing column names
- `data::Array{Any,1}`: an array rows to insert. each row is an array of values.
# Return
- `err::UInt`: status of the batch insert
"""
function cqlbatchwrite(session::Ptr{CassSession}, table::String, columns::Array{String}, data::Array{Any,1})
    # check to see that size of data and columns match
    length(columns) != length(data[1]) && (println("Data elements do not match columns."); return -1)
    query = cqlstrprep(table, columns, data)
    future = cql_session_prepare(session, query)
    cql_future_wait(future)
    err = cqlfuturecheck(future, "Session Prepare") 
    if err != CQL_OK 
        cql_future_free(future)
        return err::UInt
    end
    
    prep = cql_future_get_prepared(future)
    cql_future_free(future)
    batch = cql_batch_new(0x00)
    types = Array{DataType, 2}(1, length(data[1]))
    for i in 1:length(data[1])
        types[i] = typeof(data[1][i])
    end
    for i in 1:length(data)
        statement = cql_prepared_bind(prep)
        for j in 1:length(data[1])
            cqlstatementbind(statement, j-1, types[j], data[i][j])
        end
        cql_batch_add_statement(batch, statement)
        cql_statement_free(statement)
    end
    future = cql_session_execute_batch(session, batch)
    cql_prepared_free(prep)
    cql_future_wait(future)
    err = cqlfuturecheck(future, "Execute Batch")
    cql_future_free(future)
    cql_batch_free(batch)
    return err::UInt16
end

"""
    function cqlbatchwrite(session::Ptr{CassSession}, table::String, columns::Array{String}, data::Array{Any,1}, batchsize::Int = 1000)
batch insert very large data arrays using async writes
- `session::Ptr{CassSession}`: pointer to the active session
- `table::String`: the name of the table you want to write to
- `columns::Array{String}`: an array of existing column names
- `data::Array{Any,1}`: an array rows to insert. each row is an array of values.
- `batchsize::Int`: how many rows to send at a time
# Return
- `err::Array{UInt}`: status of the batch inserts
"""
function cqlasyncwrite(session::Ptr{CassSession}, table::String, columns::Array{String}, data::Array{Any,1}, batchsize::Int = 1000)
    datasize = length(data)
    para = datasize ÷ batchsize
    err = Array{UInt16}(para)
    @sync for i in 1:para
        to = i * batchsize
        fr = to - batchsize + 1
        if i < para
            @async err[i] = cqlbatchwrite(session, table, columns, data[fr:to])
        else
            @async err[i] = cqlbatchwrite(session, table, columns, data[fr:end])
        end
    end
    return err
end

end
