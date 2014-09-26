var mod_fs = require('fs');

var mod_cassandra = require('../lib/cassandra');
var mod_bunyan = require('bunyan');
var mod_jsonfile = require('jsonfile');
var mod_vasync = require('vasync');
var mod_underscore = require('underscore');

var LOG = mod_bunyan.createLogger({
    name: 'cassandra-test',
    src: true,
    level: 'info'
});

var CASSANDRA;
var LIST = mod_jsonfile.readFileSync('./test/etc/test-data.json');

exports.beforeTest = function (t) {
    CASSANDRA = mod_cassandra.createDatastore({
        cassandra: {
            keyspace: makeid(),
            table: makeid(),
            contactPoints: ['localhost'],
            host: 'localhost',
            port: 9042,
            cql_version: '3.2.0',
            username: 'cassandra',
            password: 'cassandra',
        },
        log: LOG
    }, function (err, res) {
        LOG.info({err: err, res: res});
        if (err) {
            t.fail(err);
        }
        t.done();
    });
};

exports.addList = function (t) {
    var barrier = mod_vasync.barrier();
    barrier.on('drain', function () {
        CASSANDRA.list(null, function(err, res) {
            LOG.info({err: err, res: res});
            if (err) {
                t.fail(err);
            }
            t.ok(res, LIST);
            t.done();
        });
    });
    Object.keys(LIST).forEach(function (key) {
        barrier.start(key);
        CASSANDRA.put(key, LIST[key], function (err) {
            if (err) {
                t.fail(err);
            }
            barrier.done(key);
        });
    });
};


exports.CRUD = function (t) {
    mod_vasync.pipeline({funcs: [
        function _create(_, _cb) {
            CASSANDRA.put('foo', 'bar', function (err, res) {
                if (err) {
                    t.fail(err);
                }

                return _cb(err);
            });
        },
        function _read(_, _cb) {
            CASSANDRA.get('foo', function (err, res) {
                if (err) {
                    t.fail(err);
                } else {
                    t.equal(res, 'bar');
                }

                return _cb(err);
            });
        },
        function _update(_, _cb) {
            CASSANDRA.put('foo', 'baz', function (err, res) {
                if (err) {
                    t.fail(err);
                }
                CASSANDRA.get('foo', function (err, res) {
                    if (err) {
                        t.fail(err);
                    } else {
                        t.equal(res, 'baz');
                    }

                    return _cb(err);
                });
            });
        },
        function _delete(_, _cb) {
            CASSANDRA.del('foo', function (err, res) {
                if (err) {
                    t.fail(err);
                }
                CASSANDRA.get('foo', function (err, res) {
                    if (err) {
                        t.fail(err);
                    }
                    t.equal(null, res);
                    return _cb();
                });
            });
        }
    ], args: {}}, function (err, res) {
        t.done();
    });

};

exports.list = function (t) {
    CASSANDRA.list(null, function(err, res) {
        LOG.info({err: err, res: res});
        if (err) {
            t.fail(err);
        }

        t.done();
    });
};

exports.afterTest = function (t) {
    CASSANDRA.disconnect(function () {
        t.done();
    });
};

/// Utils
function makeid()
{
    var text = "";
    var possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    for( var i=0; i < 5; i++ )
    text += possible.charAt(Math.floor(Math.random() * possible.length));

    return text;
}
