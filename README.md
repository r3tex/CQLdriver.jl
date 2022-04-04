# CQLdriver

[![Build Status](https://travis-ci.com/r3tex/CQLdriver.jl.svg?branch=master)](https://travis-ci.com/r3tex/CQLdriver.jl)

This Julia package is an interface to ScyllaDB / Cassandra and is based on the Datastax [CPP driver](http://datastax.github.io/cpp-driver/) implementing the CQL v3 binary protocol. The package is missing very many features, but it does two things quite well:

 - write very many rows quickly
 - read very many rows quickly

Now, it's probably easy to extend this package to enable other features, but I haven't taken the time to do so. If you find this useful but are missing a small set of features I can probably implement them if you file an issue. CQLdriver is compatible and depends on [DataFrames](https://github.com/JuliaData/DataFrames.jl) and [JuliaDB](https://github.com/JuliaComputing/JuliaDB.jl).

Currently the following data-types are supported:

|Julia Type|CQL type|
| :--- | ---: |
|String | TEXT|
|String | VARCHAR|
|Date | DATE|
|Int8 | TINYINT |
|Int16 | SMALLINT |
|Int32 | INTEGER|
|Int64 | BIGINT|
|Int64 | COUNTER|
|Bool | BOOLEAN|
|Float32 | FLOAT|
|Float64 | DOUBLE|
|DateTime | TIMESTAMP|
|UUID | UUID|

# Example use

### Starting / Closing a session
`cqlinit()` will return a tuple with 2 pointers and a `UInt16` error code which you can check. 
If the returned value is `0` then you're in good shape.
It also lets you tune some performance characteristics of your connection.
```
julia> session, cluster, err = cqlinit("192.168.1.128, 192.168.1.140")
julia> const CQL_OK = 0x0000
julia> @assert err == CQL_OK
julia> cqlclose(session, cluster)

julia> hosts = "192.168.1.128, 192.168.1.140"
julia> session, cluster, err = cqlinit(hosts, threads = 1, connections = 2, 
                                       queuesize = 4096, bytelimit = 65536, requestlimit = 256,
                                       username="admin", password="s3cr!t")
julia> cqlclose(session, cluster)
```
The driver tries to be smart about detecting all the nodes in the cluster and keeping the connection alive.

### Writing data
`cqlwrite()` takes a `DataFrame` with named columns, or a `JuliaDB` table.
Make sure that the column names in your DataFrame are the same as those in table you are writing to.
By default it will write 1000 rows per batch and will make 5 attemps at writing each batch.

For appending new rows to tables:
```
julia> table = "data.refrigerator"
julia> data = DataFrame(veggies = ["Carrots", "Broccoli"], amount = [3, 5])
julia> err = cqlwrite(session, table, data)
```
For updating a table you must provide additional arguments. 
Consider the following statement which updates a table that uses counters:
`UPDATE data.car SET speed = speed + ?, temp = temp + ? WHERE partid = ?`
The query below is analogous to the statement above:
```
julia> table = "data.car"
julia> data = DataFrame(speed=[1,2], temp=[4,5], partid=["wheel1","wheel2"])
julia> err = cqlwrite(session, 
                      table, 
                      data[:,[:speed, :total]],
                      update = data[:,[:partid]],
                      batchsize = 10000,
                      retries = 6,
                      counter = true)
```

### Reading data
`cqlread()` pulls down data in 10000-row pages by default.
It will do 5 retries per page and collate everything into a `DataFrame` with typed and named columns.
```
julia> query = "SELECT * FROM data.car"
julia> err, output = cqlread(session, query)

(0x0000, 2×3 DataFrames.DataFrame
│ Row │ speed │ temp │ partid   │
├┼┼┼┤
│ 1   │ 1     │ 4    │ "wheel1" │
│ 2   │ 2     │ 5    │ "wheel2" │)
```
Changing the page size might affect performance.
You can also increase the number of characters allowed for string types.
```
julia> query = "SELECT * FROM data.bigtable LIMIT 1000000"
julia> err, output = cqlread(session, 
                             query, 
                             pgsize = 15000, 
                             retries = 6, 
                             strlen = 1024)
```
You can send in an array of different queries and the driver will execute them asynchronously and return an array of resulting dataframes.
```
julia> query = ["SELECT * FROM data.bigtable WHERE driver=124","SELECT * FROM data.smalltable WHERE car=144"]
julia> err, output = cqlread(session, 
                             query, 
                             concurrency=500, 
                             timeout = 12000)

```


### Executing commands
`cqlexec()` runs your command on the database and returns a 0x0000 if everything went OK.
```
julia> cmd = "CREATE TABLE test.example (id int, data text, PRIMARY KEY (id));"
julia> err = cqlexec(session, cmd)
```
