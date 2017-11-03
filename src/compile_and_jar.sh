javac -classpath `hadoop classpath` -d build CountingIndexer.java &&
jar -cvf CountingIndexer.jar -C build/ .
