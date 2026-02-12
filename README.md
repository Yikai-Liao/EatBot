# 食堂预约机器人

## 1. 项目目标
- 基于飞书机器人和飞书多维表格，实现工作日自动发起食堂预约。
- 用户通过消息卡片勾选午餐/晚餐，系统自动写入和更新用餐记录。
- 在每餐截止后自动发送统计结果给指定接收人。

## 2. 业务范围与约束
- 数据存储：仅使用飞书多维表格，不使用本地数据库。
- 运行方式：Python + uv。
- 网络约束：不依赖固定公网 IP。
- 配置原则：
- 不在配置中固定 field_id。
- 通过字段名动态匹配 field_id。
- 私密配置与共享配置分离（`config.local.toml` / `config.shared.toml`）。

## 3. 业务规则（条理化）
### 3.1 每日预约卡片发送
- 在配置日期内，每天固定时间向启用用户发送预约卡片。
- 卡片含两个勾选项：午餐、晚餐。
- 默认勾选状态来源于“用餐人员配置”表中的“餐食偏好”。

### 3.2 记录创建与更新
- 卡片发送后，按默认勾选在“用餐记录”表创建记录。
- 用户修改勾选后：
- 若取消，更新该餐对应记录为“取消预约”（或业务约定状态）。
- 若选择，创建缺失记录或把记录更新为正确餐食类型。

### 3.3 截止时间控制
- 每天在配置截止时间后，不允许修改卡片状态。
- 若单卡无法对午晚餐分别控制可编辑状态，则拆分为两张卡片发送。

### 3.4 餐次统计发送
- 每顿饭截止后，统计该餐用餐人数。
- 给“统计信息接收人员”表中的人员发送统计消息。

### 3.5 日期选择优先级
- 默认规则：仅周一到周五发送，周末不发。
- 覆盖规则：以“用餐定时配置”表为最高优先级。
- 当命中开始/结束日期闭区间时，以“当日餐食包含”决定是否发卡和发哪一餐。

## 4. 数据表与字段
## 4.1 表链接
- 用餐人员配置：https://ycnw20znloxr.feishu.cn/wiki/QC07wNez9iSgZhk6rg7cOW2PnNb?table=tblrulMIAx0vnkHu&view=vewIkQTtpb
- 用餐定时配置：https://ycnw20znloxr.feishu.cn/wiki/QC07wNez9iSgZhk6rg7cOW2PnNb?table=tblUONtouxmxvVFq&view=vewaTe1QWZ
- 用餐记录：https://ycnw20znloxr.feishu.cn/wiki/QC07wNez9iSgZhk6rg7cOW2PnNb?table=tblBkttZl5XmmFFB&view=vew7J3ypSr
- 统计信息接收人员：https://ycnw20znloxr.feishu.cn/wiki/QC07wNez9iSgZhk6rg7cOW2PnNb?table=tbl6brK6FcgCynAm&view=vewt6jEAWP

## 4.2 资源标识
- wiki_token: `QC07wNez9iSgZhk6rg7cOW2PnNb`
- app_token: `PFD8b1u0jaos9vsKavBcHC4EnTg`
- table_id:
- user_config: `tblrulMIAx0vnkHu`
- meal_schedule: `tblUONtouxmxvVFq`
- meal_record: `tblBkttZl5XmmFFB`
- stats_receivers: `tbl6brK6FcgCynAm`

## 4.3 已确认字段结构（2026-02-12）
### 用餐人员配置（tblrulMIAx0vnkHu）
- `fld2XqfsGo` `用餐人员名称` `type=20`
- `fldlr2M9tU` `人员` `type=11`
- `fldg2XhEP0` `餐食偏好` `type=4`
- `fld3P7jB62` `午餐单价` `type=2`
- `fldwEjynQo` `晚餐单价` `type=2`
- `fldk0hgjQ5` `启用` `type=7`

### 用餐定时配置（tblUONtouxmxvVFq）
- `fldYA7adAt` `开始日期` `type=5`
- `fldEMgVCpg` `截止日期` `type=1`
- `fldeGCuA2Y` `当日餐食包含` `type=4`
- `fld162yoYG` `备注` `type=1`

### 用餐记录（tblBkttZl5XmmFFB）
- `fldE9D2MuW` `ID` `type=1005`
- `fldrJ7l9JF` `日期` `type=5`
- `fldPwtIqe4` `用餐者` `type=11`
- `fldfFIuqF2` `餐食类型` `type=3`
- `fldIb4SZGr` `价格` `type=1`

### 统计信息接收人员（tbl6brK6FcgCynAm）
- `fldgCAPSrk` `ID` `type=1005`
- `fldTkktZ2h` `人员` `type=11`

## 5. 配置文件说明
- `config.shared.toml`：可提交，保存 app_token、table_id、字段名映射。
- `config.local.toml`：本地私密，保存 app_id、app_secret。
- 加载建议：先加载 `config.shared.toml`，再用 `config.local.toml` 覆盖。

## 6. 技术方案
- 事件接收：飞书长连接（WebSocket）模式。
- 消息发送：飞书 IM 消息卡片。
- 数据访问：Bitable OpenAPI（records / fields）。
- 调度策略：进程内定时任务 + 截止时间判定。
- 数据一致性：写入前按“日期+人员+餐食类型”幂等检查。

## 7. 验收标准
- 每日按规则发卡，无重复、无漏发。
- 截止时间前可改、截止后不可改。
- 勾选变更可正确反映到“用餐记录”表。
- 每餐截止后统计人数正确，并可发送到接收人。
- 表字段改名后，仅修改 `config.shared.toml` 可恢复运行。

## 8. 详细开发计划
### 阶段 1：工程骨架与配置加载
- 建立项目目录：`src/`、`services/`、`domain/`、`adapters/`。
- 实现配置加载器：合并 `config.shared.toml` + `config.local.toml`。
- 启动时做配置校验（必填项、重复项、字段名空值）。

### 阶段 2：飞书客户端与鉴权
- 封装 token 管理：自动获取和刷新 tenant_access_token。
- 封装 Bitable API 客户端（fields/list、records/list、create、update）。
- 封装 IM 客户端（发送卡片、发送文本统计）。

### 阶段 3：字段名动态解析
- 启动时拉取四张表 fields。
- 按 `config.shared.toml` 的字段名映射生成 runtime field_id 映射。
- 校验字段唯一性与类型，不通过则启动失败并输出明确错误。

### 阶段 4：业务读模型
- 读取并构建：人员配置模型、定时配置模型、统计接收人模型。
- 实现日期决策器：默认规则 + 覆盖规则优先级。
- 产出当天发送计划（不发/发午餐/发晚餐/发两餐）。

### 阶段 5：预约卡片发送与落库
- 按计划给启用用户发卡。
- 依据默认偏好初始化勾选。
- 同步创建用餐记录（幂等）。

### 阶段 6：卡片回调处理
- 处理勾选变更事件。
- 在截止时间内执行更新；超过截止时间拒绝修改并返回提示。
- 更新策略：取消改状态，勾选则创建或修正记录。

### 阶段 7：餐次统计任务
- 为午餐/晚餐分别建立截止后统计任务。
- 聚合可用记录并计算人数。
- 向统计接收人发送消息。

### 阶段 8：测试与回归
- 单元测试：日期决策、字段映射、幂等写入、截止判断。
- 集成测试：对接飞书沙箱/测试表进行全流程验证。
- 回归清单：改字段名、改表 ID、跨天任务、重复事件。

### 阶段 9：上线与运维
- 增加结构化日志与错误告警。
- 输出运维手册：常见错误码、配置变更流程、排障步骤。
- 设定发布流程：灰度验证 -> 全量启用。
