A erlang key/vlaue storage-engine based on LMDB.

API
---

every key-value must be in a named sub-db

* `open/1` : equivalent to `lmdb:open(DirName, 10485760)`
* `close/1`: closes the lmdb database
* `ls/1`: list all sub-db
* `put/3`: inserts/update Key with value Val into the sub-db
* `get/2`: retrieves the value stored with Key in the sub-db
* `del/2`: Removes the key-value pair with key from the sub-db
* `count/2`: Count the key-value number for the sub-db
* `drop/2`: deletes all key-value pairs in the sub-db.

the 1st argument is always a reference as handle for lmdb database;
the 2nd argument is a {Subdb, Key} tuple, or a single SubDB string/bitstring;

when put the **FIRST** data item in a sub-db, the key type determine the sub-db's key type;

- if key is a integer, it means a integer-key sub-db;
- if key is a string, it means a binary-key sub-db;

Usage
-----

```
$ make app shell

%% open or create a lmdb database
1> D = elmdb:open("./testdb").

%% insert the key <<"a">> with value <<"1">> into sub-db "layer1"
2> elmdb:put(D, {"layer1", <<"a">>}, <<"1">>).

%% insert the integer key 789 with value <<"789">> into sub-db "layer2"
3> elmdb:put(D, {"layer2", 789}, <<"789">>).

%% retrieve the value for key <<"a">> in sub-db "layer1"
4> elmdb:get(D, {"layer1", <<"a">>}).

%% retrieve the value by id=789, from sub-db "layer2"
5> elmdb:get(D, {"layer2", 789}).

%% list all sub-db
6> elmdb:ls(D).

%% count the data-item number of sub-db "layer1"
7> elmdb:count(D, "layer1")

%% delete key <<"a">> from sub-db "layer1"
8> elmdb:del(D, {"layer1", <<"a">>}).

%% drop sub-db "layer2"
9> elmdb:drop(D, "layer2").

%% close lmdb explicitly, not necessary because of garbage-collection
10> lmdb:close(D).
```
