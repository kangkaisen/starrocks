---
displayed_sidebar: "Chinese"
---

# hours_add

## 功能

给指定的日期时间或日期增加指定的小时数。

## 语法

```Haskell
DATETIME hours_add(DATETIME|DATE date, INT hours);
```

## 参数说明

`date`: 指定的时间，支持的数据类型为 DATETIME 或者 DATE。

`hours`: 增加的小时数，支持的数据类型为 INT。

## 返回值说明

返回值的数据类型为 DATETIME。

如果 `date` 或 `hours` 任意一者为 NULL，则返回 NULL。

## 示例

```Plain Text
select hours_add('2022-01-01 01:01:01', 2);
+-------------------------------------+
| hours_add('2022-01-01 01:01:01', 2) |
+-------------------------------------+
| 2022-01-01 03:01:01                 |
+-------------------------------------+

select hours_add('2022-01-01 01:01:01', -1);
+--------------------------------------+
| hours_add('2022-01-01 01:01:01', -1) |
+--------------------------------------+
| 2022-01-01 00:01:01                  |
+--------------------------------------+

select hours_add('2022-01-01', 1);
+----------------------------+
| hours_add('2022-01-01', 1) |
+----------------------------+
| 2022-01-01 01:00:00        |
+----------------------------+
```