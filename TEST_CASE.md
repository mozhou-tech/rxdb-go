# 测试用例文档

本文档参考 [RxDB 测试用例结构](https://github.com/pubkey/rxdb/tree/master/test/unit)，列出了 rxdb-go 项目的完整测试用例规划。

## 测试覆盖概览

| 模块 | 测试文件 | 状态 | 覆盖率 |
|------|---------|------|--------|
| Database | `database_test.go` | ✅ 大部分实现 | ~90% |
| Collection | `collection_test.go` | ✅ 大部分实现 | ~85% |
| Document | `document_test.go` | ✅ 大部分实现 | ~90% |
| Query | `query_test.go` | ✅ 大部分实现 | ~85% |
| Schema Validation | `validator_test.go` | ✅ 大部分实现 | ~85% |
| Encryption | `encryption_test.go` | ✅ 已实现 | ~80% |
| Index | `index_test.go` | ✅ 已实现 | ~90% |
| Attachment | `attachment_test.go` | ✅ 已实现 | ~90% |
| Migration | `migration_test.go` | ✅ 已实现 | ~85% |
| Hooks | `hooks_test.go` | ✅ 已实现 | ~85% |

---

## 1. Database 测试用例 (`database_test.go`)

### 1.1 数据库创建和打开

- [x] `TestDatabase_CreateDatabase` - 创建新数据库
  - ✅ 创建数据库成功
  - ✅ 数据库文件正确创建
  - ✅ 返回的数据库实例不为 nil
  
- [x] `TestDatabase_CreateDatabaseWithPassword` - 使用密码创建数据库
  - ✅ 使用密码创建数据库
  - ✅ 密码正确存储
  - ✅ 数据库可正常使用

- [x] `TestDatabase_CreateDatabaseDuplicate` - 创建重复数据库
  - ✅ 使用 `ignoreDuplicate` 选项
  - ✅ 使用 `closeDuplicates` 选项
  - ✅ 默认行为（拒绝重复）

- [x] `TestDatabase_OpenExistingDatabase` - 打开已存在的数据库
  - ✅ 打开已存在的数据库文件
  - ✅ 数据正确加载
  - ✅ 集合正确恢复

### 1.2 数据库管理

- [x] `TestDatabase_Name` - 获取数据库名称
  - ✅ 返回正确的数据库名称

- [x] `TestDatabase_Close` - 关闭数据库
  - ✅ 关闭后无法执行操作
  - ✅ 资源正确释放
  - ✅ 多次关闭不报错

- [x] `TestDatabase_Destroy` - 销毁数据库
  - ✅ 数据库文件被删除
  - ✅ 所有资源被释放
  - ✅ 销毁后无法使用

- [x] `TestDatabase_RemoveDatabase` - 静态删除数据库
  - ✅ 删除数据库文件
  - ✅ 清理相关资源

### 1.3 集合管理

- [x] `TestDatabase_Collection` - 创建集合
  - ✅ 创建集合成功
  - ✅ 集合名称正确
  - ✅ Schema 正确应用

- [x] `TestDatabase_CollectionDuplicate` - 创建重复集合
  - ✅ 同名集合处理
  - ✅ Schema 兼容性检查

- [x] `TestDatabase_MultipleCollections` - 多个集合
  - ✅ 创建多个集合
  - ✅ 集合之间隔离
  - ✅ 独立操作不影响

### 1.4 数据导出导入

- [x] `TestDatabase_ExportJSON` - 导出数据库
  - ✅ 导出所有集合
  - ✅ JSON 格式正确
  - ✅ 数据完整性

- [x] `TestDatabase_ImportJSON` - 导入数据库
  - ✅ 导入数据成功
  - ✅ 集合正确创建
  - ✅ 文档正确导入

- [x] `TestDatabase_ExportImportRoundTrip` - 导出导入往返
  - ✅ 导出后导入数据一致
  - ✅ 所有集合和文档恢复

### 1.5 备份和恢复

- [x] `TestDatabase_Backup` - 备份数据库
  - ✅ 备份文件创建
  - ✅ 备份文件可读
  - ✅ 备份数据完整

- [x] `TestDatabase_RestoreFromBackup` - 从备份恢复
  - ✅ 从备份文件恢复
  - ✅ 数据正确恢复

### 1.6 变更监听

- [x] `TestDatabase_Changes` - 数据库级变更监听
  - ✅ 监听所有集合的变更
  - ✅ 变更事件正确发送
  - ✅ 事件包含正确信息

### 1.7 多实例支持

- [x] `TestDatabase_MultiInstance` - 多实例支持
  - ✅ 创建多个同名实例
  - ✅ 事件共享
  - ✅ 实例隔离

- [x] `TestDatabase_WaitForLeadership` - 等待主实例
  - ✅ 单实例下立即返回
  - [ ] 多实例选举（待实现）

- [x] `TestDatabase_RequestIdle` - 请求空闲
  - ✅ 等待活跃操作完成
  - ✅ 正确计数操作

---

## 2. Collection 测试用例 (`collection_test.go`)

### 2.1 基本 CRUD（已部分实现）

- [x] `TestCollection_Insert` - 插入文档
  - ✅ 插入成功
  - ✅ 文档 ID 正确
  - ✅ 数据正确保存
  - ✅ 修订号生成

- [x] `TestCollection_InsertDuplicate` - 插入重复文档
  - ✅ 插入相同 ID 的文档应失败
  - ✅ 错误类型正确

- [x] `TestCollection_Upsert` - 更新或插入
  - ✅ 首次插入成功
  - ✅ 更新成功
  - ✅ 修订号更新

- [x] `TestCollection_UpsertWithConflict` - 冲突处理
  - ✅ 修订号冲突处理
  - ✅ 冲突解决策略

- [x] `TestCollection_Remove` - 删除文档
  - ✅ 删除成功
  - ✅ 文档不再存在
  - ✅ 删除后查询返回 nil

- [x] `TestCollection_All` - 获取所有文档
  - ✅ 返回所有文档
  - ✅ 数量正确

- [x] `TestCollection_FindByID` - 按 ID 查找
  - ✅ 查找存在的文档
  - ✅ 查找不存在的文档返回 nil
  - ✅ ID 类型处理

- [x] `TestCollection_Count` - 统计文档数
  - ✅ 空集合返回 0
  - ✅ 有文档时返回正确数量
  - ✅ 删除后数量更新

### 2.2 批量操作

- [x] `TestCollection_BulkInsert` - 批量插入
  - ✅ 批量插入多个文档
  - ✅ 所有文档成功插入
  - ✅ 事务性（全部成功或全部失败）
  - [ ] 性能测试

- [x] `TestCollection_BulkInsertDuplicate` - 批量插入重复
  - ✅ 部分重复的处理
  - ✅ 错误处理

- [x] `TestCollection_BulkUpsert` - 批量更新或插入
  - ✅ 混合插入和更新
  - [ ] 批量操作性能

- [x] `TestCollection_BulkRemove` - 批量删除
  - ✅ 批量删除多个文档
  - ✅ 部分不存在的处理
  - [ ] 事务性

### 2.3 增量更新

- [x] `TestCollection_IncrementalUpsert` - 增量更新或插入
  - ✅ 使用 patch 更新
  - ✅ 只更新指定字段
  - ✅ 不存在的文档自动创建

- [x] `TestCollection_IncrementalModify` - 增量修改
  - ✅ 使用 modifier 函数
  - ✅ 原子性操作
  - ✅ 错误处理

### 2.4 数据导出导入

- [x] `TestCollection_ExportJSON` - 导出集合
  - ✅ 导出所有文档
  - ✅ JSON 格式正确
  - [ ] 加密字段处理

- [x] `TestCollection_ImportJSON` - 导入集合
  - ✅ 导入文档成功
  - ✅ 覆盖现有文档
  - [ ] 批量导入性能

- [x] `TestCollection_ExportImportRoundTrip` - 导出导入往返
  - ✅ 数据一致性
  - [ ] 加密字段正确

### 2.5 Dump 和 ImportDump

- [x] `TestCollection_Dump` - 导出集合（含附件）
  - ✅ 导出文档和附件
  - ✅ 格式正确

- [x] `TestCollection_ImportDump` - 导入集合（含附件）
  - ✅ 导入文档和附件
  - ✅ 附件正确恢复（在 DumpImportRoundTrip 中验证）

- [x] `TestCollection_DumpImportRoundTrip` - Dump 导入往返
  - ✅ 完整数据恢复
  - ✅ 文档和附件都正确恢复
  - ✅ 数据完整性验证

### 2.6 变更监听（已部分实现）

- [x] `TestCollection_Changes` - 变更事件流
  - ✅ Insert 事件
  - ✅ Update 事件
  - ✅ Delete 事件
  - [ ] 事件顺序
  - [ ] 并发安全
  - [ ] Channel 关闭处理

- [x] `TestCollection_ChangesMultipleListeners` - 多个监听者
  - ✅ 多个 channel 接收事件
  - ✅ 事件广播

- [x] `TestCollection_ChangesFilter` - 过滤变更
  - ✅ 按集合过滤
  - ✅ 按操作类型过滤

---

## 3. Document 测试用例 (`document_test.go`)

### 3.1 基本属性

- [x] `TestDocument_ID` - 获取文档 ID
  - ✅ 返回正确的主键值
  - [ ] 复合主键处理

- [x] `TestDocument_Data` - 获取文档数据
  - ✅ 返回完整数据
  - ✅ 数据不可变（副本）

### 3.2 字段访问

- [x] `TestDocument_Get` - 获取字段值
  - ✅ 获取存在的字段
  - ✅ 获取不存在的字段返回 nil
  - ✅ 类型转换

- [x] `TestDocument_GetString` - 获取字符串字段
  - ✅ 字符串类型返回
  - ✅ 非字符串类型转换
  - ✅ 不存在的字段返回空字符串

- [x] `TestDocument_GetInt` - 获取整数字段
  - ✅ 整数类型返回
  - ✅ 浮点数转换
  - ✅ 字符串转换

- [x] `TestDocument_GetFloat` - 获取浮点数字段
  - ✅ 浮点数返回
  - ✅ 整数转换

- [x] `TestDocument_GetBool` - 获取布尔字段
  - ✅ 布尔值返回
  - ✅ 类型转换

- [x] `TestDocument_GetArray` - 获取数组字段
  - ✅ 数组返回
  - ✅ 类型检查

- [x] `TestDocument_GetObject` - 获取对象字段
  - ✅ 对象返回
  - ✅ 类型检查

### 3.3 文档更新

- [x] `TestDocument_Set` - 设置字段值
  - ✅ 设置字段成功
  - ✅ 不保存到数据库
  - ✅ 类型验证

- [x] `TestDocument_Update` - 更新文档
  - ✅ 更新多个字段
  - ✅ 保存到数据库
  - ✅ 修订号更新
  - ✅ 变更事件发送

- [x] `TestDocument_Save` - 保存文档
  - ✅ 保存修改到数据库
  - ✅ 修订号更新

- [x] `TestDocument_Remove` - 删除文档
  - ✅ 删除成功
  - ✅ 变更事件发送

### 3.4 原子操作

- [x] `TestDocument_AtomicUpdate` - 原子更新
  - ✅ 使用函数更新
  - ✅ 原子性保证
  - [ ] 冲突处理

- [x] `TestDocument_AtomicPatch` - 原子补丁
  - ✅ 原子性更新
  - ✅ 部分字段更新

- [x] `TestDocument_IncrementalModify` - 增量修改
  - ✅ 使用 modifier 函数
  - ✅ 错误处理

- [x] `TestDocument_IncrementalPatch` - 增量补丁
  - ✅ 部分字段更新
  - ✅ 嵌套字段更新

### 3.5 JSON 转换

- [x] `TestDocument_ToJSON` - 转换为 JSON
  - ✅ JSON 格式正确
  - ✅ 所有字段包含
  - [ ] 加密字段处理

- [x] `TestDocument_ToMutableJSON` - 转换为可变 JSON
  - ✅ 返回可修改的 map
  - ✅ 数据完整性

### 3.6 变更监听

- [x] `TestDocument_Changes` - 文档变更监听
  - ✅ 监听文档变更
  - ✅ 事件正确发送
  - [ ] Channel 关闭

- [x] `TestDocument_GetFieldChanges` - 字段变更监听
  - ✅ 监听特定字段
  - ✅ 字段变更事件
  - ✅ 旧值和新值

### 3.7 状态检查

- [x] `TestDocument_Deleted` - 检查删除状态
  - ✅ 未删除返回 false
  - ✅ 已删除返回 true

---

## 4. Query 测试用例 (`query_test.go`)

### 4.1 基本查询（已部分实现）

- [x] `TestQuery_Find` - 基本查询
  - ✅ 简单字段匹配
  - ✅ 操作符查询
  - ✅ 多条件查询

- [x] `TestQuery_FindOne` - 查找单个文档
  - ✅ 找到第一个匹配
  - ✅ 未找到返回 nil

- [x] `TestQuery_Sort` - 排序
  - ✅ 升序排序
  - ✅ 降序排序
  - ✅ 多字段排序
  - [ ] 排序稳定性

- [x] `TestQuery_LimitSkip` - 分页
  - ✅ Limit 限制
  - ✅ Skip 跳过
  - ✅ Limit + Skip 组合

- [x] `TestQuery_Count` - 统计数量
  - ✅ 总数统计
  - ✅ 条件统计

### 4.2 查询操作符

#### 比较操作符

- [x] `TestQuery_Operator_Eq` - 等于
  - ✅ 基本等于查询

- [x] `TestQuery_Operator_Ne` - 不等于
  - ✅ 不等于查询
  - [ ] 空值处理

- [x] `TestQuery_Operator_Gt` - 大于
  - ✅ 数字大于
  - [ ] 字符串大于
  - [ ] 日期大于

- [x] `TestQuery_Operator_Gte` - 大于等于
  - ✅ 数字大于等于
  - ✅ 边界值

- [x] `TestQuery_Operator_Lt` - 小于
  - ✅ 数字小于
  - ✅ 边界值

- [x] `TestQuery_Operator_Lte` - 小于等于
  - ✅ 数字小于等于
  - ✅ 边界值

#### 数组操作符

- [x] `TestQuery_Operator_In` - 在数组中
  - ✅ 值在数组中
  - [ ] 空数组处理

- [x] `TestQuery_Operator_Nin` - 不在数组中
  - ✅ 值不在数组中

- [x] `TestQuery_Operator_All` - 包含所有元素
  - ✅ 数组包含所有指定元素

- [x] `TestQuery_Operator_ElemMatch` - 数组元素匹配
  - ✅ 数组元素满足条件
  - ✅ 嵌套对象匹配

- [x] `TestQuery_Operator_Size` - 数组大小
  - ✅ 数组长度匹配

#### 字符串操作符

- [x] `TestQuery_Operator_Regex` - 正则匹配
  - ✅ 基本正则匹配
  - [ ] 复杂正则表达式
  - [ ] 正则错误处理

#### 逻辑操作符

- [x] `TestQuery_Operator_And` - 逻辑与
  - ✅ 多个条件 AND
  - [ ] 嵌套 AND

- [x] `TestQuery_Operator_Or` - 逻辑或
  - ✅ 多个条件 OR
  - [ ] 嵌套 OR
  - [ ] AND 和 OR 组合

- [x] `TestQuery_Operator_Not` - 逻辑非
  - ✅ 否定条件
  - [ ] 嵌套 NOT

- [x] `TestQuery_Operator_Nor` - 逻辑或非
  - ✅ NOR 条件

#### 其他操作符

- [x] `TestQuery_Operator_Exists` - 字段存在
  - ✅ 字段存在检查
  - [ ] 字段不存在检查
  - [ ] null 值处理

- [x] `TestQuery_Operator_Type` - 类型匹配
  - ✅ 字符串类型
  - ✅ 数字类型
  - ✅ 布尔类型
  - [ ] 数组类型
  - [ ] 对象类型

- [x] `TestQuery_Operator_Mod` - 取模运算
  - ✅ 数字取模
  - [ ] 边界情况

### 4.3 链式查询

- [x] `TestQuery_Chain` - 链式查询构建
  - ✅ Where().Equals()
  - ✅ Where().Gt().Lt()
  - ✅ 链式组合

- [x] `TestQuery_SortMultipleFields` - 多字段排序
  - ✅ 多条件链式
  - ✅ 排序和分页组合

### 4.4 查询观察

- [x] `TestQuery_Observe` - 观察查询结果
  - ✅ 初始值发送
  - ✅ 变更时更新
  - ✅ 结果实时更新

- [x] `TestQuery_ObserveMultiple` - 多个观察者
  - ✅ 多个观察者独立
  - ✅ 事件正确分发

### 4.5 查询更新和删除

- [x] `TestQuery_Update` - 查询结果更新
  - ✅ 批量更新匹配文档
  - ✅ 更新条件验证

- [x] `TestQuery_Remove` - 查询结果删除
  - ✅ 批量删除匹配文档
  - ✅ 删除条件验证

### 4.6 索引优化

- [x] `TestQuery_IndexUsage` - 索引使用
  - ✅ 查询使用索引
  - ✅ 索引选择优化
  - ✅ 索引列表验证
  - [ ] 性能对比

- [x] `TestQuery_CompositeIndex` - 复合索引
  - ✅ 多字段索引使用
  - ✅ 前缀匹配
  - ✅ 完全匹配验证

---

## 5. Schema Validation 测试用例 (`validator_test.go`)

### 5.1 基本验证

- [x] `TestValidator_RequiredFields` - 必需字段
  - ✅ 缺少必需字段失败
  - ✅ 所有必需字段存在成功

- [x] `TestValidator_TypeValidation` - 类型验证
  - ✅ 字符串类型
  - ✅ 数字类型
  - ✅ 整数类型
  - ✅ 布尔类型
  - ✅ 数组类型
  - ✅ 对象类型
  - [ ] null 类型
  - [ ] 类型数组（联合类型）

### 5.2 字符串约束

- [x] `TestValidator_StringMaxLength` - 最大长度
  - ✅ 超过最大长度失败
  - ✅ 等于最大长度成功
  - ✅ 小于最大长度成功

- [x] `TestValidator_StringMinLength` - 最小长度
  - ✅ 小于最小长度失败
  - ✅ 等于最小长度成功
  - ✅ 大于最小长度成功

- [x] `TestValidator_StringPattern` - 正则表达式模式
  - ✅ 匹配模式成功
  - ✅ 不匹配模式失败
  - [ ] 无效正则表达式错误
  - [ ] 常见模式（邮箱、URL等）

### 5.3 数字约束

- [x] `TestValidator_NumberMaximum` - 最大值
  - ✅ 超过最大值失败
  - ✅ 等于最大值成功
  - ✅ 小于最大值成功

- [x] `TestValidator_NumberMinimum` - 最小值
  - ✅ 小于最小值失败
  - ✅ 等于最小值成功
  - ✅ 大于最小值成功

- [x] `TestValidator_IntegerType` - 整数类型
  - ✅ 浮点数失败
  - ✅ 整数成功

### 5.4 数组约束

- [x] `TestValidator_ArrayMinItems` - 最小元素数
  - ✅ 少于最小元素数失败
  - ✅ 等于最小元素数成功

- [x] `TestValidator_ArrayMaxItems` - 最大元素数
  - ✅ 超过最大元素数失败
  - ✅ 等于最大元素数成功

- [x] `TestValidator_ArrayItems` - 数组元素验证
  - ✅ 元素类型验证
  - ✅ 嵌套验证

### 5.5 对象验证

- [x] `TestValidator_ObjectProperties` - 对象属性验证
  - ✅ 属性类型验证
  - ✅ 嵌套对象验证
  - ✅ 属性必需性

### 5.6 默认值

- [x] `TestValidator_ApplyDefaults` - 应用默认值
  - ✅ 缺失字段应用默认值
  - ✅ 已有字段不覆盖
  - [ ] 嵌套默认值

### 5.7 不可变字段

- [x] `TestValidator_FinalFields` - 不可变字段
  - ✅ 创建后不能修改
  - ✅ 修改失败
  - ✅ 错误信息正确

### 5.8 错误报告

- [x] `TestValidator_ValidationError` - 验证错误
  - ✅ 错误类型正确
  - ✅ 错误消息清晰
  - ✅ 错误路径正确

- [x] `TestValidator_ValidateDocumentWithPath` - 详细错误路径
  - ✅ 返回所有错误
  - ✅ 错误路径正确
  - ✅ 嵌套字段路径

### 5.9 复合主键

- [x] `TestValidator_CompositePrimaryKey` - 复合主键验证
  - ✅ 所有主键字段必需
  - ✅ 主键字段类型验证

---

## 6. Encryption 测试用例 (`encryption_test.go`)

### 6.1 字段加密（已部分实现）

- [x] `TestEncryptDecryptField` - 字段加密解密
  - ✅ 加密成功
  - ✅ 解密成功
  - ✅ 加密值不同

- [x] `TestEncryptDecryptDocumentFields` - 文档字段加密
  - ✅ 指定字段加密
  - ✅ 其他字段不加密
  - ✅ 解密后数据一致

- [x] `TestCollectionWithEncryption` - 集合加密集成
  - ✅ 插入时自动加密
  - ✅ 读取时自动解密
  - ✅ 数据完整性

- [x] `TestEncryptionWithoutPassword` - 无密码加密
  - ✅ 无密码时不加密
  - ✅ 功能正常

### 6.2 加密算法

- [x] `TestEncryption_Algorithm` - 加密算法验证
  - ✅ AES-GCM 算法
  - ✅ 密钥派生
  - ✅ IV 生成（nonce）
  - ✅ Base64 编码验证
  - ✅ 相同明文不同密文（随机 nonce）

- [x] `TestEncryption_KeyDerivation` - 密钥派生
  - ✅ 相同密码生成相同密钥
  - ✅ 不同密码生成不同密钥
  - ✅ 密钥一致性验证

### 6.3 嵌套字段加密

- [x] `TestEncryption_NestedFields` - 嵌套字段加密
  - ✅ 嵌套对象字段加密
  - ✅ 字段路径正确
  - ✅ 多层嵌套支持

### 6.4 加密性能

- [x] `TestEncryption_Performance` - 加密性能
  - ✅ 大量数据加密性能（1000 次迭代）
  - ✅ 大文本加密（10KB）
  - [ ] 内存使用

### 6.5 加密错误处理

- [x] `TestEncryption_ErrorHandling` - 错误处理
  - ✅ 空密码处理
  - ✅ 无效 base64 处理
  - ✅ 损坏数据解密
  - ✅ 空字段路径错误
  - ✅ 不存在字段处理

---

## 7. Index 测试用例 (`index_test.go`)

### 7.1 索引创建

- [x] `TestIndex_CreateIndex` - 创建索引
  - ✅ 单字段索引
  - ✅ 复合索引
  - ✅ 索引名称唯一性

- [x] `TestIndex_CreateIndexDuplicate` - 创建重复索引
  - ✅ 同名索引处理
  - ✅ 相同字段索引处理

- [x] `TestIndex_CreateIndexOnExistingData` - 在现有数据上创建索引
  - ✅ 索引数据构建
  - ✅ 性能测试

### 7.2 索引查询

- [x] `TestIndex_QueryWithIndex` - 使用索引查询
  - ✅ 索引选择
  - ✅ 查询性能提升
  - ✅ 结果正确性

- [x] `TestIndex_CompositeIndexQuery` - 复合索引查询
  - ✅ 完全匹配
  - ✅ 前缀匹配
  - ✅ 部分字段匹配

### 7.3 索引维护

- [x] `TestIndex_MaintainOnInsert` - 插入时维护索引
  - ✅ 新文档索引更新
  - ✅ 索引数据正确

- [x] `TestIndex_MaintainOnUpdate` - 更新时维护索引
  - ✅ 字段变更索引更新
  - ✅ 索引数据正确

- [x] `TestIndex_MaintainOnDelete` - 删除时维护索引
  - ✅ 删除文档索引清理
  - ✅ 索引数据正确

### 7.4 索引管理

- [x] `TestIndex_ListIndexes` - 列出索引
  - ✅ 返回所有索引
  - ✅ 索引信息正确

- [x] `TestIndex_DropIndex` - 删除索引
  - ✅ 删除索引成功
  - ✅ 索引数据清理
  - ✅ 删除后查询正常

### 7.5 索引性能

- [x] `TestIndex_Performance` - 索引性能
  - ✅ 有索引 vs 无索引性能
  - ✅ 大量数据索引性能
  - ✅ 索引构建时间

---

## 8. Attachment 测试用例 (`attachment_test.go`)

### 8.1 附件操作

- [x] `TestAttachment_PutAttachment` - 添加附件
  - ✅ 添加附件成功
  - ✅ 附件元数据正确
  - ✅ 附件数据存储

- [x] `TestAttachment_GetAttachment` - 获取附件
  - ✅ 获取存在的附件
  - ✅ 获取不存在的附件失败
  - ✅ 附件数据完整

- [x] `TestAttachment_RemoveAttachment` - 删除附件
  - ✅ 删除附件成功
  - ✅ 删除后无法获取

- [x] `TestAttachment_GetAllAttachments` - 获取所有附件
  - ✅ 返回文档的所有附件
  - ✅ 附件列表正确

### 8.2 附件元数据

- [x] `TestAttachment_Metadata` - 附件元数据
  - ✅ ID、名称、类型
  - ✅ 大小、摘要
  - ✅ 创建和修改时间

### 8.3 附件大小和类型

- [x] `TestAttachment_LargeAttachment` - 大附件
  - ✅ 大文件附件
  - ✅ 性能测试

- [x] `TestAttachment_DifferentTypes` - 不同类型附件
  - ✅ 文本文件
  - ✅ 图片文件
  - ✅ 二进制文件

### 8.4 附件与文档

- [x] `TestAttachment_WithDocument` - 文档附件集成
  - ✅ 文档删除时附件清理
  - ✅ 附件与文档关联

### 8.5 Dump 和 ImportDump

- [x] `TestAttachment_Dump` - 导出附件
  - ✅ Dump 包含附件
  - ✅ 附件数据完整

- [x] `TestAttachment_ImportDump` - 导入附件
  - ✅ 导入附件成功
  - ✅ 附件数据恢复

---

## 9. Migration 测试用例 (`migration_test.go`)

### 9.1 Schema 版本

- [x] `TestMigration_SchemaVersion` - Schema 版本
  - ✅ 获取当前版本
  - ✅ 版本比较

### 9.2 迁移策略

- [x] `TestMigration_MigrationStrategy` - 迁移策略
  - ✅ 定义迁移策略
  - ✅ 执行迁移
  - ✅ 数据转换正确

- [x] `TestMigration_MultipleVersions` - 多版本迁移
  - ✅ 从旧版本逐步迁移
  - ✅ 中间版本处理

### 9.3 自动迁移

- [x] `TestMigration_AutoMigration` - 自动迁移
  - ✅ 创建集合时自动迁移
  - ✅ 版本检测

### 9.4 手动迁移

- [x] `TestMigration_ManualMigration` - 手动迁移
  - ✅ 调用 Migrate 方法
  - ✅ 迁移结果验证

### 9.5 迁移错误处理

- [x] `TestMigration_ErrorHandling` - 错误处理
  - ✅ 迁移失败回滚
  - ✅ 错误信息

- [x] `TestMigration_NoVersion` - 无版本处理
  - ✅ 无版本时不执行迁移

- [x] `TestMigration_SkipVersions` - 跳过版本
  - ✅ 跳过中间版本的迁移

---

## 10. Hooks 测试用例 (`hooks_test.go`)

### 10.1 插入钩子

- [x] `TestHooks_PreInsert` - 插入前钩子
  - ✅ 钩子执行
  - ✅ 修改文档数据
  - ✅ 阻止插入

- [x] `TestHooks_PostInsert` - 插入后钩子
  - ✅ 钩子执行
  - ✅ 访问插入的文档

### 10.2 保存钩子

- [x] `TestHooks_PreSave` - 保存前钩子
  - ✅ Insert 和 Update 都触发
  - ✅ 修改数据

- [x] `TestHooks_PostSave` - 保存后钩子
  - ✅ Insert 和 Update 都触发
  - ✅ 访问保存的文档

### 10.3 删除钩子

- [x] `TestHooks_PreRemove` - 删除前钩子
  - ✅ 钩子执行
  - ✅ 阻止删除

- [x] `TestHooks_PostRemove` - 删除后钩子
  - ✅ 钩子执行
  - ✅ 访问删除的文档

### 10.4 创建钩子

- [x] `TestHooks_PreCreate` - 创建前钩子
  - ✅ 集合创建前执行
  - ✅ 修改 Schema

- [x] `TestHooks_PostCreate` - 创建后钩子
  - ✅ 集合创建后执行
  - ✅ 访问集合

### 10.5 多个钩子

- [x] `TestHooks_MultipleHooks` - 多个钩子
  - ✅ 多个钩子顺序执行
  - ✅ 钩子之间数据传递

### 10.6 钩子错误处理

- [x] `TestHooks_ErrorHandling` - 错误处理
  - ✅ 钩子返回错误
  - ✅ 操作取消
  - ✅ 错误传播

- [x] `TestHooks_ConcurrentHooks` - 并发钩子
  - ✅ 并发安全
  - ✅ 钩子正确计数

---

## 11. 集成测试

### 11.1 端到端测试

- [ ] `TestIntegration_FullWorkflow` - 完整工作流
  - 创建数据库和集合
  - 插入、查询、更新、删除
  - 数据一致性

- [ ] `TestIntegration_ConcurrentOperations` - 并发操作
  - 并发插入
  - 并发查询
  - 数据一致性

- [ ] `TestIntegration_Transaction` - 事务性
  - 批量操作事务性
  - 失败回滚

### 11.2 性能测试

- [ ] `TestPerformance_LargeDataset` - 大数据集
  - 插入大量数据
  - 查询性能
  - 内存使用

- [ ] `TestPerformance_ConcurrentQueries` - 并发查询
  - 多个并发查询
  - 性能测试

### 11.3 压力测试

- [ ] `TestStress_HighLoad` - 高负载
  - 持续高负载操作
  - 资源使用
  - 稳定性

---

## 12. 错误处理测试

### 12.1 错误类型

- [ ] `TestErrors_ValidationError` - 验证错误
  - 错误类型识别
  - 错误信息

- [ ] `TestErrors_NotFoundError` - 未找到错误
  - 文档不存在
  - 集合不存在

- [ ] `TestErrors_AlreadyExistsError` - 已存在错误
  - 重复插入
  - 重复创建

- [ ] `TestErrors_ClosedError` - 已关闭错误
  - 数据库关闭后操作
  - 集合关闭后操作

### 12.2 错误恢复

- [ ] `TestErrors_Recovery` - 错误恢复
  - 从错误中恢复
  - 数据一致性

---

## 测试运行说明

### 运行所有测试

```bash
go test ./...
```

### 运行特定测试

```bash
# 运行 Collection 测试
go test ./pkg/rxdb -run TestCollection

# 运行 Query 测试
go test ./pkg/rxdb -run TestQuery

# 运行加密测试
go test ./pkg/rxdb -run TestEncryption
```

### 运行测试并查看覆盖率

```bash
go test -cover ./...
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

### 运行基准测试

```bash
go test -bench=. ./...
```

---

## 测试覆盖率目标

- **核心功能**: 90%+
- **查询功能**: 85%+
- **验证功能**: 90%+
- **加密功能**: 85%+
- **整体覆盖率**: 80%+

---

## 参考资源

- [RxDB 测试用例](https://github.com/pubkey/rxdb/tree/master/test/unit)
- [Go 测试文档](https://golang.org/pkg/testing/)
- [测试最佳实践](https://github.com/golang/go/wiki/TestComments)

