(function(module) {
    var async  = require("async"),
        crypto = require("crypto"),
        mess   = require("mess"),
        events = require("events"),
        util   = require("util"),
        nop    = require("nop");

    // emits: ready, error

    function Queue(redises, zk, key, w) {
        var self = this;

        self.redises = redises;
        self.zk      = zk;
        self.key     = key;
        self.w       = w || 1;
        self.ready   = false;
        self.closed  = false;

        if (!self.redises.length) {
            throw new Error("No redis servers provided");
        }

        // start everything inside
        self.redises.forEach(function(redis) {
            redis.on("error", self.emit.bind(self, "error"));
        });

        function zkRestart() {
            if (self.closed) {
                return;
            }

            self.zk.start(function(error) {
                if (error) {
                    self.emit("error", error);
                    setTimeout(zkRestart, 1000);
                } else {
                    self.redises.forEach(function(redis) {
                        if (!redis.retry_max_delay || redis.retry_max_delay > 1000) {
                            self.emit("error", new Error("Redis retry_max_delay is too big to be fast if redis is dead"));
                        }
                    });

                    self.ready = true;
                    self.emit("ready");
                }
            });
        }

        self.zk.on("expired", zkRestart);

        zkRestart();
    }

    util.inherits(Queue, events.EventEmitter);

    Queue.prototype.getRedises = function() {
        return mess(this.redises.slice());
    };

    Queue.prototype.push = function(item, callback) {
        var self = this,
            adders;

        if (!callback) {
            callback = nop;
        }

        if (self.closed) {
            callback(new Error("Trying to push to closed queue"));
        }

        adders = self.getRedises().map(function(redis) {
            return function(callback) {
                redis.sadd(self.key, item, function(error) {
                    if (error) {
                        self.emit("error", error);
                    }

                    callback(undefined, error ? false : true);
                });
            };
        });

        async.parallel(adders, function(error, added) {
            added = added.reduce(function(count, success) {
                return success ? (count + 1) : count;
            }, 0);

            if (added >= self.w) {
                callback(undefined, added);
            } else {
                callback(new Error("Could not write to " + self.w + " redis servers!"));
            }
        });
    };

    Queue.prototype.getLockName = function(item) {
        return "/" + this.key + crypto.createHash("md5").update(item).digest("hex");
    };

    /**
     * Callback will receive third argument as a function to call
     * when processing of item is finished. Not required if no item returned.
     * @param {Function} callback
     * @param {Number} tries
     */
    Queue.prototype.pop = function(callback, tries) {
        var self    = this,
            redises = self.getRedises(),
            item,
            lock;

        if (!tries) {
            tries = 1;
        }

        if (self.closed) {
            callback(new Error("Trying to pop from closed queue"));
        }

        async.doUntil(function(callback) {
            redises.pop().srandmember(self.key, function(error, random) {
                if (error) {
                    self.emit("error", error);
                } else {
                    item = random;
                }

                callback();
            });
        }, function() {
            return redises.length == 0 || item !== undefined;
        }, function() {
            if (redises.length == 0 && item === undefined) {
                callback(new Error("No available redis servers found"));
                return;
            }

            // no item found
            if (item == null) {
                callback();
                return;
            }

            lock = self.getLockName(item);
            self.zk.create(lock, "", self.zk.create.EPHEMERAL, function(error) {
                if (self.zk.errors.NODEEXISTS == error) {
                    if (tries > 1) {
                        // maybe next time
                        self.pop(callback, tries - 1);
                    } else {
                        callback();
                    }

                    return;
                }

                if (error) {
                    callback(error);
                    return;
                }

                callback(undefined, item, function(callback) {
                    if (!item) {
                        return;
                    }

                    if (!callback) {
                        callback = nop;
                    }

                    self.remove(item, function(error) {
                        if (error) {
                            self.emit("error", error);
                        }

                        self.unlock(item, callback);
                    });
                }, function(callback) {
                    if (!item) {
                        return;
                    }

                    if (!callback) {
                        callback = nop;
                    }

                    self.unlock(item, callback);
                });
            });
        });
    };

    Queue.prototype.remove = function(item, callback) {
        var self = this;

        if (!callback) {
            callback = nop;
        }

        if (self.closed) {
            callback(new Error("Trying to remove item from closed queue"));
        }

        async.parallel(self.getRedises().map(function(redis) {
            return function(callback) {
                redis.srem(self.key, item, function(error) {
                    if (error) {
                        self.emit("error", error);
                    }

                    callback(undefined, error ? true : false);
                });
            }
        }), callback);
    };

    Queue.prototype.unlock = function(item, callback) {
        var self = this,
            lock = self.getLockName(item);

        if (!callback) {
            callback = nop;
        }

        if (self.closed) {
            callback(new Error("Trying to unlock item in closed queue"));
        }

        self.zk.del(lock, -1, function(error) {
            if (error) {
                self.emit("error");
            }

            callback();
        });
    };

    Queue.prototype.getSize = function(callback) {
        var self    = this,
            redises = self.getRedises(),
            result;

        if (self.closed) {
            callback(new Error("Trying to get size of closed queue"));
        }

        async.doUntil(function(callback) {
            redises.pop().scard(self.key, function(error, size) {
                if (error) {
                    self.emit("error", error);
                } else {
                    result = size;
                }

                callback();
            });
        }, function() {
            return redises.length == 0 || result !== undefined;
        }, function() {
            callback(result === undefined ? new Error("Cannot get queue size") : undefined, result);
        });
    };

    Queue.prototype.clear = function(callback) {
        var self = this;

        if (!callback) {
            callback = nop;
        }

        if (self.closed) {
            callback(new Error("Trying to clear closed queue"));
        }

        async.parallel(self.getRedises().map(function(redis) {
            return function(callback) {
                redis.del(self.key, function(error) {
                    if (error) {
                        self.emit("error", error);
                    }

                    callback(undefined, error ? false : true);
                });
            }
        }), function(error, removed) {
            removed = removed.reduce(function(count, success) {
                return success ? (count + 1) : count;
            }, 0);

            if (removed >= self.w) {
                callback(undefined, removed);
            } else {
                callback(new Error("Failed to clear queue"));
            }
        });
    };

    Queue.prototype.close = function() {
        var self = this;

        self.closed = true;
        self.ready  = false;
        self.zk.close();
        self.redises.forEach(function(redis) {
            redis.quit();

            // if it's broken, let's close it this way
            redis.once("error", function(error) {
                self.emit("error", error);

                redis.stream.once("error", self.emit.bind(self));
                redis.end();
            });
        });
    };

    module.exports = Queue;
})(module);
