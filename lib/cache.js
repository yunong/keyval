var mod_assert = require('assert-plus');
var mod_once = require('once');
var mod_vasync = require('vasync');

function Cache(opts, cb) {
    mod_assert.object(opts, 'opts');
    mod_assert.number(opts.ttl, 'opts.ttl');
    mod_assert.object(opts.datastore, 'opts.datastore');
    mod_assert.object(opts.log, 'opts.log');
    mod_assert.func(cb, 'cb');

    this._cache = {};
    this._ttl = opts.ttl;
    this._datastore = opts.datastore;
    this._log = opts.log.child({ component: 'cache' });

    var self = this;
    var log = self._log;

    cb = mod_once(cb);
    (function populateCache() {
        log.info('populating cache');
        self._datastore.list(null, function (err, res) {
            log.info({err: err}, 'done populating cache');
            if (res) {
                self._cache = res;
            }

            setTimeout(populateCache, self._ttl);
        });
    })();

    return cb();
}
module.exports = {
    createCache: function (opts, cb) {
        return new Cache(opts, cb);
    }
};

Cache.prototype.put = function put(key, value, cb) {
    var self = this;
    var cache = self._cache;
    var log = self._log;
    var datastore = self._datastore;

    log.info('Cache.put: entering');
    mod_vasync.pipeline({funcs: [
        function _putDSKey(_, _cb) {
            datastore.put(key, value, _cb);
        },
        function _putCache(_, _cb) {
            cache[key] = value;
            return _cb();
        }
    ], arg:{}}, function (err, res) {
        log.info({err: err}, 'Cache.put: exiting');
        return cb(err);
    });
};

Cache.prototype.get = function get(key, cb) {
    var self = this;
    self._log.info('Cache.get: entering');

    return cb(null, self._cache[key]);
};

Cache.prototype.del = function del(key, cb) {
    var self = this;
    var datastore = self._datastore;
    var cache = self._cache;
    var log = self._log;

    log.info('Cache.del: entering');
    mod_vasync.pipeline({funcs: [
        function _delDSKey(_, _cb) {
            datastore.del(key, _cb);
        },
        function _delCache(_, _cb) {
            if (cache[key]) {
                delete cache[key];
            }

            return _cb();
        }
    ], arg: {}}, function (err, res) {
        log.info({err: err}, 'Cache.del: entering');
        return cb(err);
    });
};

Cache.prototype.list = function list(stream, cb) {
    // TODO: implement streaming
    var self = this;
    self._log.info('Cache.list: entering');

    return cb(null, self._cache);
};
