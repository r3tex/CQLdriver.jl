if !is_linux()
    error("This package does not support OSX or Windows")
end

version = "2.8.1"
const has_driver = !isempty(Libdl.find_library(["libcassandra"]))
const has_yum = try success(`yum --version`) catch e false end
const has_apt = try success(`apt-get -v`) && success(`apt-cache -v`) catch e false end

if has_driver
    println("Cassandra CPP driver already installed.")
elseif has_yum
    url = "http://downloads.datastax.com/cpp-driver/centos/7/cassandra/v" * version * "/"
    file = "cassandra-cpp-driver-" * version * "-1.el7.centos.x86_64.rpm"
    source = url * file
    target = "/tmp/cassandra-cpp-driver.rpm"
    dl = try success(`wget -O $target $source`) catch e false end
    !dl && error("Unable to download CPP driver.")
    inst = try success(`sudo yum install -y $target`) catch e false end
    !inst && error("Unable to install CPP driver.")
elseif has_apt
    url = "http://downloads.datastax.com/cpp-driver/ubuntu/16.04/cassandra/v" * version * "/"
    file = "cassandra-cpp-driver_" * version * "-1_amd64.deb"
    source = url * file
    target = "/tmp/cassandra-cpp-driver.deb"
    uv = try success(`sudo apt install -y libuv0.10`) catch e false end
    !uv && error("Unable to install libuv.")
    dl = try success(`wget -O $target $source`) catch e false end
    !dl && error("Unable to download CPP driver.")
    inst = try success(`sudo dpkg -i $target`) catch e false end
    !inst && error("Unable to install CPP driver.")
else
    error("This package requires cassandra-cpp-driver to be installed, but the build system only understands apt and yum.")
end



