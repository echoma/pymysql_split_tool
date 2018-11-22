SHELL := /bin/bash
BIN := ../pymysql_split_tool/

all: test

test: cli_test api_test api_monkey_patch_test

cli_test: test_case_01 test_case_02 test_case_03 test_case_04 test_case_05 test_case_06 test_case_07

api_test: test_case_21 test_case_22 test_case_23 test_case_24 test_case_25 test_case_26 test_case_27

api_monkey_patch_test: test_case_41 test_case_42 test_case_43 test_case_44 test_case_45 test_case_46 test_case_47

prepare:
	#create a brand new table for test
	mysql --defaults-extra-file=./my.cnf -e 'drop database if exists `test`; create database `test` default charset utf8;'
	mysql --defaults-extra-file=./my.cnf -e 'use test; create table test (id int auto_increment, data_int int not null default 0, data_int2 int not null default 0, data_varchar varchar(200), data_bin blob, primary key (id)) ENGINE=InnoDB'
	#insert test record into test table
	for n in {1001..1020}; do mysql --defaults-extra-file=./my.cnf -e "use test; insert into test values($$n, $$n*10, 20140102, '\$$\#\@\%\&\*\'\"\\\?\<\>\:\;', X'00010203');"; done
	for n in {1021..1040}; do mysql --defaults-extra-file=./my.cnf -e "use test; insert into test values($$n, $$n*10, 20140117, '\$$\#\@\%\&\*\'\"\\\?\<\>\:\;', X'00010203');"; done
	for n in {1041..1060}; do mysql --defaults-extra-file=./my.cnf -e "use test; insert into test values($$n, $$n*10, 20150329, '\$$\#\@\%\&\*\'\"\\\?\<\>\:\;', X'00010203');"; done
	for n in {1061..1100}; do mysql --defaults-extra-file=./my.cnf -e "use test; insert into test values($$n, $$n*10, 20151205, '\$$\#\@\%\&\*\'\"\\\?\<\>\:\;', X'00010203');"; done

do_cli_test = echo -e "\033[32m=========\nStart $(1)\n=========\033[0m" && \
				make prepare && \
				python $(BIN) --action split --task ./$(1).json && \
				echo -e "\033[32mChecking spliting result\033[0m" && \
				python ./check_test_result.py --action split --case $(1) && \
				echo -e "\033[32mChecking with action check\033[0m" && \
				python $(BIN) --action check --task ./$(1).json &&\
				echo -e "\033[32mRemoving with action remove\033[0m" && \
				python $(BIN) --action remove --task ./$(1).json &&\
				echo -e "\033[32mChecking remove result\033[0m" && \
				python ./check_test_result.py --action remove --case $(1)

do_api_test = echo -e "\033[32m=========\nStart $(1)\n=========\033[0m" && \
				make prepare && \
				python ./api_test.py --action split --task ./$(2) && \
				echo -e "\033[32mChecking spliting result\033[0m" && \
				python ./check_test_result.py --action split --case $(2) && \
				echo -e "\033[32mChecking with action check\033[0m" && \
				python ./api_test.py --action check --task ./$(2) &&\
				echo -e "\033[32mRemoving with action remove\033[0m" && \
				python ./api_test.py --action remove --task ./$(2) &&\
				echo -e "\033[32mChecking remove result\033[0m" && \
				python ./check_test_result.py --action remove --case $(2)

do_api_monkey_patch_test = echo -e "\033[32m=========\nStart $(1)\n=========\033[0m" && \
				make prepare && \
				python ./api_test.py --action split --task ./$(2) --monkey_patch && \
				echo -e "\033[32mChecking spliting result\033[0m" && \
				python ./check_test_result.py --action split --case $(2) && \
				echo -e "\033[32mChecking with action check\033[0m" && \
				python ./api_test.py --action check --task ./$(2) --monkey_patch &&\
				echo -e "\033[32mRemoving with action remove\033[0m" && \
				python ./api_test.py --action remove --task ./$(2) --monkey_patch &&\
				echo -e "\033[32mChecking remove result\033[0m" && \
				python ./check_test_result.py --action remove --case $(2)

test_case_01:
	$(call do_cli_test,$@)

test_case_02:
	$(call do_cli_test,$@)

test_case_03:
	$(call do_cli_test,$@)

test_case_04:
	$(call do_cli_test,$@)

test_case_05:
	$(call do_cli_test,$@)

test_case_06:
	$(call do_cli_test,$@)

test_case_07:
	$(call do_cli_test,$@)



test_case_21: test_case_01
	$(call do_api_test,$@,$<)

test_case_22: test_case_02
	$(call do_api_test,$@,$<)

test_case_23: test_case_03
	$(call do_api_test,$@,$<)

test_case_24: test_case_04
	$(call do_api_test,$@,$<)

test_case_25: test_case_05
	$(call do_api_test,$@,$<)

test_case_26: test_case_06
	$(call do_api_test,$@,$<)

test_case_27: test_case_07
	$(call do_api_test,$@,$<)



test_case_41: test_case_01
	$(call do_api_monkey_patch_test,$@,$<)

test_case_42: test_case_02
	$(call do_api_monkey_patch_test,$@,$<)

test_case_43: test_case_03
	$(call do_api_monkey_patch_test,$@,$<)

test_case_44: test_case_04
	$(call do_api_monkey_patch_test,$@,$<)

test_case_45: test_case_05
	$(call do_api_monkey_patch_test,$@,$<)

test_case_46: test_case_06
	$(call do_api_monkey_patch_test,$@,$<)

test_case_47: test_case_07
	$(call do_api_monkey_patch_test,$@,$<)