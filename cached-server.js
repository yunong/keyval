var mod_bunyan = require('bunyan');
var mod_restify = require('restify');

var mod_api = require('./lib/api');
var mod_cassandra = require('./lib/cassandra.js');
var mod_cache = require('./lib/cache.js');
var mod_vasync = require('vasync');

(function main () {
    var self = this;
    var log = mod_bunyan.createLogger({
        name: 'keyval',
        level: 'info',
        src: process.env.LOG_DEBUG,
        serializers: mod_bunyan.stdSerializers
    });

    mod_vasync.pipeline({funcs:[
        function _createCassandra(_, _cb) {
            _.cassandra = mod_cassandra.createDatastore({
                cassandra: {
                    keyspace: 'yunong',
                    table: 'urls',
                    contactPoints: ['localhost'],
                    cql_version: '3.2.0',
                    username: 'cassandra',
                    password: 'cassandra',
                },
                log: log
            }, _cb);
        },
        function _createCache(_, _cb) {
            _.datastore = mod_cache.createCache({
                ttl: 5000,
                datastore: _.cassandra,
                log: log
            }, _cb);
        },
        function _start(_, _cb) {

            var server = mod_restify.createServer({
                name: 'keyval',
                log: log,
                version: '1.0.0'
            });
            var port = 1338;
            var datastore = _.datastore;
            server.use(mod_restify.queryParser());
            server.on('after', mod_restify.auditLogger({
                log: mod_bunyan.createLogger({
                    name: 'audit',
                    stream: process.stdout
                })
            }));


            server.put('/keys/:name', mod_api.putKey.bind(this, datastore));
            server.get('/keys/:name', mod_api.getKey.bind(this, datastore));
            server.del('/keys/:name', mod_api.delKey.bind(this, datastore));
            server.get('/keys', mod_api.listKeys.bind(this, datastore));

            server.listen(port);
            log.info('server started on port ' + port);
        }
    ], arg:{}}, function (err, res) {
        if (err) {
            throw err;
        }
    });
})();
