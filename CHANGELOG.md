* Fix pending query issue

## 0.1.10 ##
* Make async cursor close() sync

## 0.1.9 ##
* Ability to use root_ca

## 0.1.8 ##
* Make cursor.fetch*() sync for async cursor

## 0.1.7 ##
* Update ydb dependency to 3.8.13

## 0.1.6 ##
* Invalidate session&tx on YDB errors

## 0.1.5 ##
* Add StaticCredentials parser

## 0.1.4 ##
* Fix description update

## 0.1.3 ##
* Fix python 3.8 typing

## 0.1.2 ##
* Ability to pass credentials by string

## 0.1.1 ##
* Decrease min Python version to 3.8

## 0.1.0 ##
* Add bulk upsert to connection
* Fix README markup

## 0.0.1b8 ##
* Cherry pick settings propagation

## 0.0.1b7 ##
* Add missing param to cursor constructor

## 0.0.1b6 ##
* Fix get_table_names connection method

## 0.0.1b5 ##
* Add prefix pragma to scheme queries

## 0.0.1b4 ##
* Add table prefix pragma to queries

## 0.0.1b3 ##
* Add pool retries on execute calls

## 0.0.1b2 ##
* Fix tx modes
* Fix missing await in async connection

## 0.0.1b1 ##
* YDB DBAPI based on QueryService
