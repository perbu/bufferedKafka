# Buffered kafka writer

This implements a buffered writer. If kafka is not available it will buffer in memory is long as possible.

It will attempt to write to kafka in bursts, increasing performance.


Note that it will send a test message upon startup to ensure the kafka cluster is up and running.
