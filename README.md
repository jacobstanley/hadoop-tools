# Hadoop Tools [![Hackage version](https://img.shields.io/hackage/v/hadoop-tools.svg?style=flat)](http://hackage.haskell.org/package/hadoop-tools) [![Build Status](http://img.shields.io/circleci/project/jystic/hadoop-tools.svg?style=flat)](https://circleci.com/gh/jystic/hadoop-tools)

Tools for working with Hadoop written with performance in mind.

*This has been tested with the HDFS protocol used by CDH 5.x*

## Where can I get it?

See our latest release [v0.7.2](https://github.com/jystic/hadoop-tools/releases/tag/v0.7.2)!

## Configuration

By default, `hh` will behave the same as `hdfs dfs` or `hadoop fs` in
terms of which user name to use for HDFS, or which namenodes to use.


### User

The default is to use your current unix username when accessing HDFS.

This can be overridden either by using the `HADOOP_USER_NAME`
environment variable:

```bash
# This trick also works with `hdfs dfs` and `hadoop fs`
export HADOOP_USER_NAME=amber
```

or by adding the following to your `~/.hh` configuration file:

```config
hdfs {
  user = "amber"
}
```

### Namenode

The default is to lookup the namenode configuration from
`/etc/hadoop/conf/core-site.xml` and `/etc/hadoop/conf/hdfs-site.xml`.

This can be overridden by adding the following to your `~/.hh`
configuration file:

```config
namenode {
  host = "hostname or ip address"
}
```

or if you're using a non-standard namenode port:

```config
namenode {
  host = "hostname or ip address"
  port = 7020 # defaults to 8020
}
```

*NOTE: You cannot currently specify multiple namenodes using the `~/.hh`
config file, but this would be easy to add. If you would like this
feature then please add an
[issue](https://github.com/jystic/hadoop-tools/issues).*


### SOCKS Proxy

Sometimes it can be convenient to access HDFS over a SOCKS proxy. The
easiest way to get this to work is to connect to a server which can
access the namenode using `ssh <host> -D1080`. This sets up a SOCKS
proxy locally on port `1080` which can access everything that `<host>`
can access.

To get `hh` to make use of this proxy, add the following to your `~/.hh`
configuration file:

```config
proxy {
  host = "127.0.0.1"
  port = 1080
}
```
