using BinDeps

if !is_linux()
    error("This package does not support OSX or Windows")
end

version = "2.7.1"

has_driver = !isempty(Libdl.find_library(["libcassandra"]))
const has_yum = try success(`yum --version`) catch e false end
const has_apt = try success(`apt-get -v`) && success(`apt-cache -v`) catch e false end

if has_driver
    nothing
elseif has_yum
    url = "http://downloads.datastax.com/cpp-driver/centos/7/cassandra/v" * version * "/"
    file = "cassandra-cpp-driver-" * version * "-1.el7.centos.x86_64.rpm"
    out = "cassandra-cpp-driver.rpm"
elseif has_apt
url = "http://downloads.datastax.com/cpp-driver/ubuntu/16.04/cassandra/v" * version * "/"
file = "cassandra-cpp-driver-dbg_" * version * "-1_amd64.deb"
out = "cassandra-cpp-driver.deb"
else
    error("This package requires cassandra-cpp-driver to be installed")
end



