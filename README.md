# CQLdriver

[![Build Status](https://travis-ci.org/r3tex/CQLdriver.jl.svg?branch=master)](https://travis-ci.org/r3tex/CQLdriver.jl)

[![Coverage Status](https://coveralls.io/repos/r3tex/CQLdriver.jl/badge.svg?branch=master&service=github)](https://coveralls.io/github/r3tex/CQLdriver.jl?branch=master)

[![codecov.io](http://codecov.io/github/r3tex/CQLdriver.jl/coverage.svg?branch=master)](http://codecov.io/github/r3tex/CQLdriver.jl?branch=master)

# Intro
This Julia package for interfacing with ScyllaDB / Cassandra is based on the Datastax [CPP driver](http://datastax.github.io/cpp-driver/) implementing the CQL v3 binary protocol. 

Julia performance is on par with C++ and about twice as fast as Python. This package is missing very many features (nearly all of them), but it does two things quite well:

 - write very many rows quickly
 - read very many rows quickly

Now, it's probably easy to extend this package to enable other features, but I haven't taken the time to do so. If you find this useful but are missing a small set of features I can probably implement them if you file an issue.

# Example use

### Starting / Closing a session
`cqlinit()` will return a session::Ptr, a cluster::Ptr, and an err::UInt16
You can check the value and see if the connection was set up successfully.
```
julia> session, cluster, err = cqlinit("127.0.0.1")
julia> const CQL_OK = 0x0000
julia> @assert err == CQL_OK
julia> cqlclose(session, cluster)
```
### Writing batches of data
`cqlasyncwrite()` takes an array of arrays - rows of values
it sends 1000 rows per batch by default, and returns an array of `err`
E.g. if you write 5000 rows, it will return a 5 element array - [0,0,0,0,0]
```
julia> table = "data.testtable"
julia> data = [["hello", 1, now()],
               ["test1", 2, now()]]
julia> colnames = ["msg", "num", "timestamp"]

julia> err = cqlasyncwrite(session, table, colnames, data)
```
### Reading rows of data
`cqlread()` pulls down data 10000 rows at a time by default
it will return a tuple with an `err` and the `result` as an array of arrays
```
julia> query = "SELECT DISTINCT vals FROM data.test LIMIT 1000000"

julia> err, result = cqlread(session, query)
```

# Upcoming features
- automatic read retries
- reads return a dataframe with named columns
- registering as official Julia package

