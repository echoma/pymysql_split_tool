**What is this?**


Mysql query will become slow when table contains millions of rows. A simple way of solving this probolem is spliting the big table into small ones. We use some method to decide which table each row should be moved into. A widely used method is "modulus", e.g. group rows by [ID mod 100]. This tool will help you to do this job easily.


Check home page for usage and more infomation.


**Features**

* Move records from one table to another(or other) table(s) according to user specified rules.
* Dynamically create new tables during moving.
* Move records between different mysql servers.
* Check data integrity of the new tables.
* For records who have been moved to new tables, remove them from source table.
* Using it as a python module which means dynamically changing the behavior.