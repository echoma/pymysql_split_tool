SHELL := /bin/bash

all: test

test: test_split

test_split: test_case_01 test_case_02 test_case_03 test_case_04 test_case_05 test_case_06 test_case_07

prepare:
	#grant all  on *.* to test@'localhost' identified by  'test' with grant option;
	#create a brand new table for test
	mysql -utest -ptest -e 'drop database if exists `test`; create database `test` default charset utf8;'
	mysql -utest -ptest -e 'use test; create table test (id int auto_increment, data_int int, data_int2 int, data_varchar varchar(200), data_bin blob, primary key (id)) ENGINE=InnoDB'
	#insert test record into test table
	for n in {1001..1020}; do mysql -utest -ptest -e "use test; insert into test values($$n, unix_timestamp(), 20140102, '\$$\#\@\%\&\*\'\"\\\?\<\>\:\;', X'00010203');"; done
	for n in {1021..1040}; do mysql -utest -ptest -e "use test; insert into test values($$n, unix_timestamp(), 20140117, '\$$\#\@\%\&\*\'\"\\\?\<\>\:\;', X'00010203');"; done
	for n in {1041..1060}; do mysql -utest -ptest -e "use test; insert into test values($$n, unix_timestamp(), 20150329, '\$$\#\@\%\&\*\'\"\\\?\<\>\:\;', X'00010203');"; done
	for n in {1061..1100}; do mysql -utest -ptest -e "use test; insert into test values($$n, unix_timestamp(), 20151205, '\$$\#\@\%\&\*\'\"\\\?\<\>\:\;', X'00010203');"; done

test_case_01: prepare
	echo -e "\033[32mtest case 01\033[0m"
	python ../pymysql_split_tool.py --action split --task ./test_case_01.json

test_case_02: prepare
	echo -e "\033[32mtest case 02\033[0m"
	python ../pymysql_split_tool.py --action split --task ./test_case_02.json

test_case_03: prepare
	echo -e "\033[32mtest case 03\033[0m"
	python ../pymysql_split_tool.py --action split --task ./test_case_03.json

test_case_04: prepare
	echo -e "\033[32mtest case 04\033[0m"
	python ../pymysql_split_tool.py --action split --task ./test_case_04.json

test_case_05: prepare
	echo -e "\033[32mtest case 05\033[0m"
	python ../pymysql_split_tool.py --action split --task ./test_case_05.json

test_case_06: prepare
	echo -e "\033[32mtest case 06\033[0m"
	python ../pymysql_split_tool.py --action split --task ./test_case_06.json

test_case_07: prepare
	echo -e "\033[32mtest case 07\033[0m"
	python ../pymysql_split_tool.py --action split --task ./test_case_07.json
