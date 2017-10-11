push!(LOAD_PATH, "../src")

using CQLdriver
using DataFrames
const CQL_OK = 0x0000

session, cluster, err = cqlinit("192.168.1.115, 192.168.1.149")

rows = 100
a = now()
table = "test.benchmarks"
cols = Symbol.(["imsi", "ts", "id", "pl", "t1id", "t1rnc", "t2id", "t2rnc", "t3id", "t3rnc", "t4id", "t4rnc"])
types = [String, DateTime, Int32, Int32, Int32, Int32, Int32,
                           Int32, Int32, Int32, Int32, Int32]
data = DataFrame(types, cols, 0)
for i in 1:rows
    arraybuf = [string(i), a, Int32(1), Int32(2), Int32(3), Int32(4), Int32(5), 
                              Int32(6), Int32(7), Int32(8), Int32(9), Int32(10)]
    push!(data, arraybuf)
end

#print("Starting test in "),sleep(1),print("3"),sleep(1),print(" 2"),sleep(1),println(" 1"),sleep(1)

@time err = cqlwrite(session, table, data)
err == CQL_OK ? println("Write Successful") : println("Write Failed")

table = "data.location_stage2"
cols = Symbol.(["cars", "speed", "osm_id", "ts", "direction"])
types = [Int64, Int64, Int64, DateTime, String]
data = DataFrame(types, cols, 0)
for i in 1:rows
    arraybuf = [Int64(6), Int16(6), Int32(i), a, "TO HELL"]
    push!(data, arraybuf)
end
@time err = cqlwrite(session, table, data[:,1:2], update=data[:,3:5], counter=true)
err == CQL_OK ? println("Write Successful") : println("Write Failed")

query = "SELECT * FROM test.benchmarks LIMIT 100000"
@time err, result = cqlread(session, query)
err == CQL_OK ? println("Read Successful") : println("Read Failed")

cqlclose(session, cluster)
