(function(module) {
    var async  = require("async"),
        crypto = require("crypto"),
        mess   = require("mess"),
        events = require("events"),
        util   = require("util");

    // emits: ready, error

    function Queue(redises, zk, key, w) {
        var self = this;

        self.redises = redises;
        self.zk      = zk;
        self.key     = key;
        self.w       = w || 1;
        self.ready   = false;

        // start everything inside
        self.redises.forEach(function(redis) {
            redis.on("error", self.emit.bind(self, "error"));
        });

        function zkRestart() {
            self.zk.start(function(error) {
                if (error) {
                    self.emit("error", error);
                } else {
                    self.emit("ready");

                    self.redises.forEach(function(redis) {
                        if (!redis.retry_max_delay || redis.retry_max_delay > 1000) {
                            self.emit("error", new Error("Redis retry_max_delay is too big to be fast if redis is dead"));
                        }
                    });
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

            if (item === undefined) {
                callback();
                return;
            }

            lock = "/" + self.key + crypto.createHash("md5").update(item).digest("hex");
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

                    self.remove(item, function(error) {
                        if (error) {
                            self.emit("error", error);
                        }

                        // TODO: error? huh, that's bad
                        self.zk.del(lock, -1, function(error) {
                            if (error) {
                                self.emit("error");
                            }

                            callback();
                        });
                    });
                });
            });
        });
    };

    Queue.prototype.remove = function(item, callback) {
        var self = this;

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

    Queue.prototype.getSize = function(callback) {
        var self    = this,
            redises = self.getRedises(),
            result;


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
        })
    };

    module.exports = Queue;
})(module);
