Hadoop Notes
============

## FsShell
- Implements the 'hdfs dfs' functionality

### FsUsage.Du
- Implments the 'hdfs dfs -du' functionality

## MapTask.runNewMapper
- Creates the Mapper class and runs it in process
- This is where we would want to launch a Haskell process or call it via
  JNI


## MapTask.NewDirectOutputCollector<K, V>
- Creates a record writer for the correct output format
- Provides write(key, value) function for the MapTask
