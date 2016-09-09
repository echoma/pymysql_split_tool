# 这是什么？

Mysql在数据表包含的记录数达到百万级别后，查询会变得原来越慢。解决这类问题的一种简单办法，就是把这个大表拆成很多份小表。通常我们会使用某种方法来确认某条记录应该被移动到哪个小表里，比如取模（按照ID模100分成100张小表）。这个工具可以帮我们更轻松的完成这个工作。

# 特性

* 按照用户指定的分组规则，把数据从一个表移动记录到另一个或多个新表
* 在移动数据的过程中动态创建新表
* 可以在不同的数据库服务器之间移动数据
* 检查新数据表的数据完整性
* 对于已经移动到新表的数据，将这些数据从源表中删除
* 可以作为python模块调用，这意味着可以动态的调整工具的行为方式

# 使用方法

* 只要创建一个任务文件，然后执行这个工具即可:

`python ./pymysql_split_tool --action split --task  ./task.json`

支持以下python版本，并做了测试：`2.6, 2.7, 3.3, 3.4, 3.5`

* 命令参数:

| 参数名 | 描述 |
| -------- | -------- |
| action | 要执行什么操作. 必填. |
| task | 任务文件的路径，该文件包含了任务的具体信息. 必填. |
| debug | 打印调试日志. 可选. |

* `action`参数支持如下取值:

| action | description |
| -------- | -------- |
| split | 将数据从大表拷贝到小表. |
| check | 检查小表里的数据完整性. ==还未实现==. |
| remove | 删除原大表里的数据. ==还未实现==. |

* 一个典型的任务文件长这样:

```json
{
  "src":{
    "mysql":{
      "host":"127.0.0.1",
      "port":"3306",
      "unix_socket":"/path/to/mysqld.sock",
      "user":"user_name",
      "password":"password"
    },
    "database":"name_of_database",
    "table":"name_of_the_big_table"
  },
  "dest":{
    "table":"small_table_[n]"
  },
  "rule":{
    "filter":"id>1000",
    "group_method":"modulus",
    "group_base":100,
    "group_column":"id"
  }
}
```

### 更多例子

查看 [test](test) 目录可以看到很多例子.

### 任务文件里支持的字段:

| 第一级字段 | 第二级字段 | 第三级字段 | 描述 | 例如 |
| -------- | -------- | -------- | -------- | -------- |
| src |  |  | 将要被拆分的大表信息 |  |
|  | mysql |  | 连接数据库使用的参数（PyMysql模块） |  |
|  |  | host | mysql服务地址 | "127.0.0.1" |
|  |  | port | mysql服务端口 | 3306 |
|  |  | unix_socket | 通过unix套接字连接mysql服务 | "/var/run/mysqld.sock" |
|  |  | user | mysql用户名 | "root" |
|  |  | password | mysql密码 |  |
|  | database |  | 所在数据库名 |  |
|  | table |  | 表名 |  |
| dest |  |  | 数据被拆分到小表的信息 |  |
|  | mysql |  | mysql连接参数. <br/>如果没有设置此字段，这个工具就使用src里相同的设置. <br/>他里面的第三级参数跟src也是一样的。 |  |
|  | database |  | 在哪个数据库里创建新的小表. <br/>如果没有设置此字段，这个工具就是用src里相同的设置 |  |
|  | table |  | 新表的名字模式字符串. 里面含有的**[n]**将被一个整数替代. | "small\_table\_[n]" |
|  | create_table_first |  | 如果设置为1，新表不存在时会创建. <br/>默认为1. ==还不支持0的取值==. | 0 |
|  | create_table_sql |  | 指定创建新表的sql语句. <br/>如果没有设置, 该工具会通过`show create table`取得原大表的创建sql来创建新的小表. <br/>==目前还不能指定==. | "create table [table_name] if not exists..." |
| rule |  |  | 定义任务如何被完成 |  |
|  | filter |  | 一个sql的where声明，可以过滤出要进行拷贝的数据字段 | "id>1000" |
|  | page_size |  | 如果分页拷贝数据，这个字段就指定了每页的记录数 | 1000 |
|  | page_sleep |  | 每次拷贝1页后，暂停多少秒。这样可以防止数据库繁忙 | 1 |
|  | order_by |  | 分页拷贝时，指定排序规则 | "id asc" |
|  | group_method |  | 分组方法，每一条记录都可以按照这个方法计算出一个整数。按`模[modulus]`还是`余数[devide]`还是`全部不分组[all]` | "modulus" |
|  | group_base |  | 分组基数。用来取模或除法的数字 | 100 |
|  | group_column |  | 表里的这一列将用来做分组计算 |
|  | group_int |  | 指定整数列表. 如果设置了这个字段，本工具将只对拷贝映射到这些整数的记录.<br/>因此只对`取模`和`余数`两种分组方法有效。<br/>语法(json数组): [数字1,数字2,[起始,结束],数字3,数字4] | [1,7,[12,15],20] |
| check |  |  | `action`为`check`时专用. 指定了我们如何检查数据完整性 |  |
| | count |  | 如果为1，则使用mysql的count()函数比较源表和新表的记录数 | 1 |
| | sum | 列名列表，将使用mysql的sum()函数对源表和新表的指定列进行求和 | ["id","created"] |
