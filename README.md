# MYDB 数据库运行指南（Windows + IDEA）

## 环境要求
- Java 8 或更高版本
- Maven 3.x
- Windows 操作系统
- 项目路径：`D:\Code\Java\MYDB(demo)`

---

## 快速启动步骤

### 步骤 1：编译项目
在项目根目录下执行：
```powershell
mvn clean compile
```

### 步骤 2：创建数据库（仅首次运行）
```powershell
mvn exec:java "-Dexec.mainClass=top.dhc.mydb.backend.Launcher" "-Dexec.args=-create D:\Code\Java\MYDB(demo)\mydb"
```
**说明**：此命令会在指定路径创建数据库文件，只需执行一次。

### 步骤 3：启动数据库服务器（终端 1）
```powershell
mvn exec:java "-Dexec.mainClass=top.dhc.mydb.backend.Launcher" "-Dexec.args=-open D:\Code\Java\MYDB(demo)\mydb"
```
**说明**：
- 服务器将在 9999 端口启动
- 看到 `Server listen to port: 9999` 表示启动成功
- **保持此终端运行，不要关闭**

### 步骤 4：启动客户端（终端 2）
打开新的 PowerShell 终端，执行：
```powershell
cd D:\Code\Java\MYDB(demo)
mvn exec:java "-Dexec.mainClass=top.dhc.mydb.client.Launcher"
```
**说明**：
- 成功连接后会显示 `:>` 提示符
- 现在可以输入 SQL 命令进行操作

---

## SQL 命令示例

连接成功后，可以执行以下命令：
```sql
-- 开始事务
begin

-- 创建表
create table test id int32, value int32 (index id)

-- 插入数据
insert into test values 10 22
insert into test values 20 33

-- 查询数据
select * from test

-- 提交事务
commit

-- 退出客户端
exit
```

---

## 注意事项

### PowerShell 命令格式
- ⚠️ **重要**：在 PowerShell 中，所有 `-D` 参数必须用**双引号**包裹
- ✅ 正确：`"-Dexec.mainClass=..."`
- ❌ 错误：`-Dexec.mainClass="..."`

### 路径说明
- 数据库文件路径：`D:\Code\Java\MYDB(demo)\mydb`
- 如果路径包含空格或特殊字符（如括号），必须保持引号包裹

### 端口占用
- 默认端口：9999
- 如果端口被占用，需要先关闭占用该端口的程序

### 停止服务
- 服务器：在终端 1 按 `Ctrl + C`
- 客户端：输入 `exit` 或按 `Ctrl + C`

---

## 快速启动脚本（可选）

为了方便使用，可以创建以下批处理脚本：

### server.bat（启动服务器）
```batch
@echo off
echo 正在启动 MYDB 数据库服务器...
mvn exec:java -Dexec.mainClass=top.dhc.mydb.backend.Launcher -Dexec.args="-open D:\Code\Java\MYDB(demo)\mydb"
pause
```

### client.bat（启动客户端）
```batch
@echo off
echo 正在连接 MYDB 数据库...
mvn exec:java -Dexec.mainClass=top.dhc.mydb.client.Launcher
pause
```

### create.bat（创建数据库，仅首次使用）
```batch
@echo off
echo 正在创建 MYDB 数据库...
mvn exec:java -Dexec.mainClass=top.dhc.mydb.backend.Launcher -Dexec.args="-create D:\Code\Java\MYDB(demo)\mydb"
pause
```

**使用方法**：
1. 将上述内容保存为对应的 `.bat` 文件到项目根目录
2. 双击运行即可

---

## 常见问题

### 1. 客户端提示 "Usage: launcher (open|create) DBPath"
**原因**：服务器未启动或连接失败  
**解决**：确保先启动服务器（步骤 3），再启动客户端

### 2. 提示 "Unknown lifecycle phase"
**原因**：PowerShell 参数格式错误  
**解决**：确保 `-D` 参数用双引号包裹，如 `"-Dexec.mainClass=..."`

### 3. 端口被占用
**原因**：9999 端口已被其他程序使用  
**解决**：
```powershell
# 查看端口占用
netstat -ano | findstr :9999
# 结束占用进程
taskkill /PID <进程ID> /F
```

---

## 项目结构
```
MYDB(demo)/
├── src/
│   └── main/
│       └── java/
│           └── top/dhc/mydb/
│               ├── backend/     # 服务器端代码
│               │   └── Launcher.java
│               └── client/      # 客户端代码
│                   └── Launcher.java
├── target/                      # 编译输出目录
├── mydb/                        # 数据库文件目录
├── pom.xml                      # Maven 配置文件
└── README.md                    # 本文档
```

---

## 版本信息
- **项目名称**：MYDB
- **版本**：1.0-SNAPSHOT
- **Java 版本**：8
- **Maven 版本**：3.x

---

