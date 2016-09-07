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
	for n in {1001..1020}; do mysql -utest -ptest -e "use test; insert into test values($$n, $$n*10, 20140102, '\$$\#\@\%\&\*\'\"\\\?\<\>\:\;', X'00010203');"; done
	for n in {1021..1040}; do mysql -utest -ptest -e "use test; insert into test values($$n, $$n*10, 20140117, '\$$\#\@\%\&\*\'\"\\\?\<\>\:\;', X'00010203');"; done
	for n in {1041..1060}; do mysql -utest -ptest -e "use test; insert into test values($$n, $$n*10, 20150329, '\$$\#\@\%\&\*\'\"\\\?\<\>\:\;', X'00010203');"; done
	for n in {1061..1100}; do mysql -utest -ptest -e "use test; insert into test values($$n, $$n*10, 20151205, '\$$\#\@\%\&\*\'\"\\\?\<\>\:\;', X'00010203');"; done

do_split_test = echo -e "\033[32mStart $(1)\033[0m" && make prepare && python ../pymysql_split_tool.py --action split --task ./$(1).json && echo -e "\033[32mChecking result\033[0m" && python ./check_test_result.py --case $(1)

test_case_01:
	$(call do_split_test,$@)

test_case_02:
	$(call do_split_test,$@)

test_case_03:
	$(call do_split_test,$@)

test_case_04:
	$(call do_split_test,$@)

test_case_05:
	$(call do_split_test,$@)

test_case_06:
	$(call do_split_test,$@)

test_case_07:
	$(call do_split_test,$@)
