zk-redis-queue
====

Reliable queue that duplicate items among redis servers
and synchronize with zookeeper. Use if you don't want to loose
your data and don't mind if some items can be processed
more than 1 time if redis fails.

### Advantages

* No single point of failure (SPoF): zookeeper is distributed, redis data is duplicated.
* Losing data is impossible if you have at least 1 redis server alive.
* Processing can be paralleled horizontally if you want to process faster.

### Disadvantages

* Not strongly ordered.
* Duplicate items are the same — you need to add timestamp or something to make them different.
* Can process items more than 1 time if redis has failed and returned with non-empty set.

### Installation

```
npm install zk-redis-queue
```

### Usage

You may be interested in test/test.js for now. In this example you'll need
2 redis servers on 127.0.0.1:6379 and 127.0.0.1:6380. We'll use shared zk instance
so performance will be poor, please start your own cluster for production use.

```javascript
var Redis    = require("redis"),
    Queue    = require("../index"),
    ZK       = require("zkjs"),
    zk       = new ZK({hosts: ["api.yongwo.de:2181"], root: "/pew-pew", timeout: 2000}),
    redisOne = Redis.createClient(6379, "127.0.0.1", {retry_max_delay: 1000}),
    redisTwo = Redis.createClient(6380, "127.0.0.1", {retry_max_delay: 1000}),
    queue    = new Queue([redisOne, redisTwo], zk, "woo"); // "woo" is a queue key

// error event is triggered for recoverable errors,
// if error is unrecoverable, you'll receive error in callback
queue.on("error", function(error) {
    console.log("Got warning", error);
});

// we are ready to start
queue.on("ready", function() {
    // push item to queue
    queue.push("pew", function(error) {
        if (error) {
            // push failed on every redis server
            throw error;
        }
    });

    // let's try to get something for 5 times (second argument)
    queue.pop(function(error, item, remove) {
        if (error) {
            // no redis servers are alive
            // or zookeeper is down
            // damn!
            throw error;
        }

        if (item) {
            // we got item and it's locked for us!
            console.log("Look what I've got:", item);

            // remove item after all work is done
            remove(function() {
                // no error if everything is down,
                // "error" event will be emitter
                // and item can be processed by another worker
                console.log("Item removed!");
            });
        }
    }, 5);
});

```


### Implementation

Queue consists of 2 elements: zookeeper pool (synchronization to make parallel processing possible)
and redis pool (data storage). Redis servers are completely independent.
We suggest you to have at least 3 redis servers on different physical servers.

### Authors

* [Ian Babrou](https://github.com/bobrik)
