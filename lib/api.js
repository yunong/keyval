var mod_restify = require('restify');

module.exports = {
    putKey: function putKey(datastore, req, res, next) {
        var log = req.log;
        var err;
        if (!req.params.name) {
            err = new mod_restify.InvalidArgumentError('key name must ' +
                                                             'be supplied');
            return next(err);
        }

        if (!req.params.value) {
            err = new mod_restify.InvalidArgumentError('key value must ' +
                                                             'be supplied');
            return next(err);
        }
        datastore.put(req.params.name, req.params.value, function (err) {
            if (err) {
                log.error({err: err, key: req.params.name},
                          'unable to put key');
                return next(new mod_restify.InternalError('unable to put key'));
            }
            res.send();
            return next();
        });
    },

    getKey: function getKey(datastore, req, res, next) {
        var log = req.log;
        if (!req.params.name) {
            return next(new mod_restify.InvalidArgumentError('key name must ' +
                                                             'be supplied'));
        }

        datastore.get(req.params.name, function (err, value) {
            if (err) {
                log.error({err: err, key: req.params.name},
                          'unable to get key');
                return next(new mod_restify.InternalError('unable to get key'));
            }

            if (!value) {
                return next(new mod_restify.ResourceNotFoundError(
                    'key does notexist'));
            }

            res.send(value);
            return next();
        });
    },

    delKey: function delKey(datastore, req, res, next) {
        var log = req.log;
        if (!req.params.name) {
            return next(new mod_restify.InvalidArgumentError('key name must ' +
                                                             'be supplied'));
        }

        datastore.del(req.params.name, function (err) {
            if (err) {
                log.error({err: err, key: req.params.name},
                          'unable to del key');
                return next(new mod_restify.InternalError('unable to del key'));
            }

            res.send(204);
            return next();
        });
    },

    // TODO: implement streaming?
    listKeys:  function listKeys(datastore, req, res, next) {
        var log = req.log;
        datastore.list(null, function (err, value) {
            if (err) {
                log.error({err: err}, 'unable to list');
                return next(new mod_restify.InternalError('unable to list'));
            }

            res.send(value);
            return next();
        });
    }
};

