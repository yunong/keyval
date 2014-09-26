var mod_sprintf = require('util').format;

var mod_assert = require('assert-plus');
var mod_cassandra = require('cassandra-driver');
var mod_vasync = require('vasync');
var mod_verror = require('verror');

function Cassandra(opts, cb) {
    mod_assert.object(opts, 'opts');

    mod_assert.object(opts.cassandra, 'opts.cassandra');
    mod_assert.arrayOfString(opts.cassandra.contactPoints,
                             'opts.cassandra.contactPoints');
    mod_assert.string(opts.cassandra.keyspace, 'opts.keyspace');
    mod_assert.string(opts.cassandra.table, 'opts.table');
    mod_assert.string(opts.cassandra.cql_version, 'opts.cql_version');
    mod_assert.optionalString(opts.cassandra.user, 'opts.user');
    mod_assert.optionalString(opts.cassandra.pass, 'opts.pass');

    mod_assert.object(opts.log, 'opts.log');

    mod_assert.func(cb, 'cb');

    var self = this;

    this._log = opts.log.child({ component: 'cassandra' });
    this._cassandraOpts = opts.cassandra;
    if (opts.cassandra.username && opts.cassandra.password) {
        self._cassandraOpts.authProvider =
            new mod_cassandra.auth.PlainTextAuthProvider(
                opts.cassandra.username,
                opts.cassandra.password
            );
    }
    this._keyspace = opts.cassandra.keyspace;
    // delete the keyspace from the cassandra opts, otherwise creating the
    // keyspace will fail.
    delete opts.cassandra.keyspace;
    this._table = opts.cassandra.table;
    this._conn = new mod_cassandra.Client(self._cassandraOpts);

    self.CREATE_KEYSPACE = mod_sprintf(self.CREATE_KEYSPACE, self._keyspace);
    self.USE_KEYSPACE = mod_sprintf(self.USE_KEYSPACE, self._keyspace);
    self.CREATE_TABLE = mod_sprintf(self.CREATE_TABLE, self._table);
    self.CREATE_VALUE_INDEX = mod_sprintf(self.CREATE_VALUE_INDEX, self._table,
                                          self._table);
    self.LIST_TABLE = mod_sprintf(self.LIST_TABLE, self._table);
    self.GET = mod_sprintf(self.GET, self._table);
    self.PUT = mod_sprintf(self.PUT, self._table);
    self.DEL = mod_sprintf(self.DEL, self._table);
    self.SEARCH = mod_sprintf(self.PUT, self._table);

    var log = self._log;

    // Boostrap Cassandra
    mod_vasync.pipeline({funcs: [
        function _connect(_, _cb) {
            self._conn.connect(function (err) {
                if (err) {
                    err = new mod_verror.VError(err,
                                            'unable to connect to cassandra');
                }
                return _cb(err);
            });
        },
        function _createKeyspace(_, _cb) {
            log.info({statement: self.CREATE_KEYSPACE}, 'creating keyspace');

            self._conn.execute(self.CREATE_KEYSPACE, function (err) {
                if (err) {
                    err = new mod_verror.VError(err,
                                                'unable to create keyspace');
                }

                return _cb(err);
            });
        },
        function _useKeyspace(_, _cb) {
            log.info({statement: self.USE_KEYSPACE}, 'using keyspace');

            self._conn.execute(self.USE_KEYSPACE, function (err) {
                if (err) {
                    err = new mod_verror.VError(err, 'unable to use keyspace');
                }

                return _cb(err);
            });
        },
        function _createTable(_, _cb) {
            log.info({statement: self.CREATE_TABLE}, 'creating table');

            self._conn.execute(self.CREATE_TABLE, function (err) {
                if (err) {
                    err = new mod_verror.VError(err, 'unable to create table');
                }

                return _cb(err);
            });
        },
        function _createValueIndex(_, _cb) {
            log.info({statement: self.CREATE_VALUE_INDEX}, 'creating index');

            self._conn.execute(self.CREATE_VALUE_INDEX, function (err) {
                if (err) {
                    err = new mod_verror.VError(err, 'unable to create index');
                }

                return _cb(err);
            });
        }
    ], arg:{}}, function (err, res) {
        log.info({err: err}, 'cassandra initialized');
        return cb(err, res);
    });
}
module.exports = {
    createDatastore: function createDatastore(opts, cb) {
        return new Cassandra(opts, cb);
    }
};

Cassandra.prototype.CREATE_KEYSPACE = 'CREATE KEYSPACE IF NOT EXISTS %s WITH ' +
    'replication = {\'class\': \'SimpleStrategy\', \'replication_factor\' : 1}';
Cassandra.prototype.USE_KEYSPACE = 'USE %s';
Cassandra.prototype.CREATE_TABLE = 'CREATE TABLE IF NOT EXISTS %s ' +
    '(key text PRIMARY KEY, value text);';
Cassandra.prototype.CREATE_VALUE_INDEX = 'CREATE INDEX IF NOT EXISTS ' +
    'value_index%s ON %s (value)';
Cassandra.prototype.LIST_TABLE = 'select * from %s';
Cassandra.prototype.PUT = 'INSERT INTO %s (key, value) VALUES (?, ?);';
Cassandra.prototype.GET = 'SELECT * FROM %s where key=?';
Cassandra.prototype.DEL = 'DELETE FROM %s where key=?';
Cassandra.prototype.SEARCH = 'SELECT * FROM %s where value=?';

Cassandra.prototype.disconnect = function disconnect(cb) {
    var self = this;
    var log = self._log;

    log.info('Cassandra.disconnect: entering');

    self._conn.shutdown(function () {
        log.info('Cassandra.disconnect: exiting');
        return cb();
    });
};

Cassandra.prototype.put = function put(key, value, cb) {
    var self = this;
    var log = self._log;

    log.info({
        key: key,
        value: value,
        statement: self.PUT
    }, 'Cassandra.put: entering');

    self._conn.execute(self.PUT, [key, value], function (err, res) {
        log.info({err: err, res: res}, 'Cassandra.put: exiting');
        return cb(err, res);
    });
};

Cassandra.prototype.get = function get(key, cb) {
    var self = this;
    var log = self._log;

    log.info({
        key: key,
        statement: self.GET
    }, 'Cassandra.put: entering');

    self._conn.execute(self.GET, [key], function (err, res) {
        log.info({err: err, res: res}, 'Cassandra.get: exiting');
        var value;
        if (res && res.rows && res.rows[0]) {
            value = res.rows[0].value;
        }
        return cb(err, value);
    });
};

Cassandra.prototype.del = function del(key, cb) {
    var self = this;
    var log = self._log;

    log.info({
        key: key,
        statement: self.DEL
    }, 'Cassandra.del: entering');

    self._conn.execute(self.DEL, [key], function (err, res) {
        log.info({err: err, res: res}, 'Cassandra.del: exiting');

        return cb(err, res);
    });
};

Cassandra.prototype.list = function list(stream, cb) {
    var self = this;
    var log = self._log;

    log.info({
        statement: self.LIST_TABLE
    }, 'Cassandra.list: entering');

    var resStream = self._conn.stream(self.LIST_TABLE);
    if (stream) {
        resStream.pipe(stream);
        log.info('Cassandra.list: exiting after pipe');
        return cb();
    } else {
        var res = {};
        resStream.on('readable', function () {
            var row;
            while (null !== (row = resStream.read())) {
                log.info({row: row}, 'Cassandra.list: got row');
                res[row.key] = row.value;
            }
        }).on('end', function () {
            log.info({res: res}, 'Cassandra.list: exiting');
            return cb(null, res);
        }).on('error', function (err) {
            log.info({err: err, res: res}, 'Cassandra.list: exiting');
            return cb(err);
        });
    }
};

// TODO implement search
Cassandra.prototype.search = function search(stream, cb) {
    return cb();
};
