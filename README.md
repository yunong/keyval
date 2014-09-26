keyval
======

Key value store with a REST API

# API
```
GET /keys
GET /keys/:name
PUT /keys/:name?value=bar
DELETE /keys/:name
```

# Client Examples

## Listing all keys
```bash
bash-3.2$ curl lgwd-yunong:1337/keys
{"asdfasdfadsf":"bar","cars":"yunong","asdfdf":"bar","foo":"bar","caradadsfadsf":"yunong","asdf":"bar","mr":"foo"
```

## Putting a key
```bash
bash-3.2$ curl 'lgwd-yunong:1337/keys/o?value=yunong' -X PUT
```

## Getting a key

```bash
bash-3.2$ curl -H 'Accept: application/json' lgwd-yunong:1337/keys/o
"yunong"
```

## Delete a key

```bash
bash-3.2$ curl lgwd-yunong:1337/keys/o -X DELETE
```

