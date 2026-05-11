# JoyAgent Token 用量看板 (jd_token_bill)

一套本地运行、零云端依赖的 JoyAgent token 用量统计工具。

- **客户版**（推荐）：单页用量看板，只看自己企业的 token 调用明细 + 估算费用，登录在网页上一键完成。
- **管理版**：管理员脚本，定时轮询 + SQLite 留痕 + 京东云账单对账。

> 数据完全在本机处理：登录态写入本地 `joyagent_profile/`，账单只在内存里实时拉取，浏览器调用 JoyAgent 自己的接口（你登录后就能看到的那些）。

---

## 目录结构

```
jd_token_bill/
├── jd_token_bills_client/        ← 客户版（推荐）
│   ├── client_dashboard.py
│   └── requirements.txt
├── jd_token_bills/               ← 管理版（可选）
│   ├── joyagent_monitor.py       一直跑的轮询器，写 SQLite
│   ├── web_dashboard.py          管理后台（含三方账单对账）
│   └── requirements_monitor.txt
└── README.md                     ← 当前文件
```

---

## 环境要求

| 项 | 版本 |
|---|---|
| Windows / macOS / Linux | 任意 |
| Python | 3.10 及以上 |
| 浏览器 | 脚本会自动下载 Chromium，无需自带 |

---

## ⚡ 客户版：3 步上手

适合：你只想看自己企业的 token 用量明细，不需要管理员视图、不需要多账户、不需要历史归档。

### 1. 安装依赖（只做一次）

打开 **PowerShell**（Windows）或 **Terminal**（macOS / Linux），进入项目目录：

```powershell
cd jd_token_bills_client
pip install -r requirements.txt
python -m playwright install chromium
```

> Windows 用户如遇中文乱码，每个会话先执行一次：`$env:PYTHONIOENCODING='utf-8'; chcp 65001 | Out-Null`

### 2. 启动看板

```powershell
python client_dashboard.py
```

终端会打印类似下面的内容：

```text
============================================================
Customer dashboard:  http://127.0.0.1:8766/
Build:               2026-05-11 22:16:55 #7da7bc8f
Profile directory:   D:\tmp\jd_token_bills_client\joyagent_profile

WARNING: login profile is empty.
First-time setup: run this in another shell, then refresh the page:
    python client_dashboard.py --login
============================================================
```

浏览器会自动打开 `http://127.0.0.1:8766/`。

### 3. 在页面上完成登录

第一次访问时页面长这样：

```
                       🔐
                  请登录 JoyAgent
       登录后即可查看你所在企业的 token 用量明细。

      ┌──────────────────┐    ┌────────────────────────┐
      │   登录 JoyAgent   │    │  我已登录过，重新检查   │
      └──────────────────┘    └────────────────────────┘

   点击后会弹出 JoyAgent 登录窗口；扫码或输入账号密码完成登录后
   窗口会自动关闭，本页会自动加载数据。
```

直接点 **登录 JoyAgent**：

1. 后台会启动一个子进程拉起一个真实的 Chromium 窗口
2. 窗口里你正常扫码 / 输入账号密码登录 JoyAgent
3. 登录被检测到后窗口会自动关闭
4. 看板自动刷新，显示你的用量明细

> 已经在 CLI 里跑过 `--login` 的用户：直接点 **我已登录过，重新检查** 即可。

完成。后续启动只需执行第 2 步的 `python client_dashboard.py`。

---

## 客户版：日常使用

### 看板视图

| 区域 | 内容 |
|---|---|
| 顶栏 | 当前用户、所属企业、构建版本号（用来一眼判断浏览器有没有缓存旧版本） |
| 工具栏 | 月份预设：本月 / 上月 / 最近 3 / 6 个月；可追加任意月份；自动刷新；导出 CSV |
| 筛选区 | 资源（模型）、Token 类型、排序；有筛选时出现"清空筛选"按钮 |
| 4 张指标卡 | 查询范围、明细笔数、总 tokens、费用 |
| 按模型汇总 | 每个模型的调用次数、tokens、费用，配占比条 |
| 按 Token 类型汇总 | 输入 / 输出 / 缓存读 / 缓存写 5m / 缓存写 1h，配颜色点 + 占比条 |
| 近 30 天扣费走势 | mini 条形图，悬停显示当日金额 |
| 按天小计 | 每天的调用、tokens、费用，列出涉及的模型 |
| 用量明细 | 全部记录，支持分页、排序、按模型/Token 类型筛选 |

### 费用计算公式

```
单条费用 = tokens × 模型公开单价 (USD/M tokens) × 7
按条向下抹零到分（floor 到 0.01）
汇总按抹零后的费用相加
```

| Cell | 含义 |
|---|---|
| 「费用」 | 抹零后的费用（金额栏显示这一项） |
| 「raw 费用」 | 不抹零、保留 4 位小数（用于核对） |

> 这只是估算，京东云的实际扣费以京东云账单为准。多数情况下二者一致或相差 ≤1 分。

---

## 客户版：常用命令

| 场景 | 命令 |
|---|---|
| 启动看板 | `python client_dashboard.py` |
| 启动但不自动开浏览器 | `python client_dashboard.py --no-open` |
| 换端口 | `python client_dashboard.py --port 8888` |
| 局域网内别人也能访问 | `python client_dashboard.py --host 0.0.0.0` |
| 命令行登录（页面登录失败时备用） | `python client_dashboard.py --login` |
| 切换企业空间（看到的是个人空间时） | `python client_dashboard.py --switch-tenant` |
| 切换到指定企业 | `python client_dashboard.py --switch-tenant "广东 XX 公司"` |
| 清空登录信息（重新登录） | `python client_dashboard.py --reset` |
| 看到弹出的浏览器（默认 headless） | `python client_dashboard.py --switch-tenant --show-browser` |

完整参数 `python client_dashboard.py --help`。

---

## 客户版：常见问题

### Q1：页面看到旧版样式

**自检方法**：看顶栏右侧 `build YYYY-MM-DD HH:MM:SS #xxxxxxxx`，时间戳应该和你最近修改 / 拉取代码的时间一致。

- 如果时间不一致 → 服务进程没重启，`Ctrl+C` 后重新运行 `python client_dashboard.py`
- 如果时间一致 → 浏览器缓存还在，按 `Ctrl + F5` 强刷
- 如果还不行 → 在登录页右侧点 **刷新会话缓存** 按钮（清掉运行中浏览器 worker 的缓存）

### Q2：登录后还是显示"请登录"

通常因为运行中的服务器 worker 还在用旧 cookie。点登录页的 **我已登录过，重新检查**（它会自动调 `/api/restart-worker` 让 worker 重新读最新 cookie）。

### Q3：登录后显示"账号已登录，但未进入企业空间"

JoyAgent 默认是个人空间，需要切换到企业空间。页面会列出你可加入的企业空间列表，选一个点 **切换** 即可。

### Q4：上月 / 历史月没数据

确保左上角已选中正确的月份（点 **上月** 或在 **追加月份** 里手动加 `2026-04` 这种格式），点 **立即刷新**。如果接口确实返回 0 条，会显示"所选月份内没有任何调用记录"。

### Q5：想清空所有本地数据重来

```powershell
python client_dashboard.py --reset
```

会停掉运行中的 browser worker 并删除 `joyagent_profile/`。然后重新走"3 步上手"流程。

> 客户版 **不存任何账单 SQLite**，所有账单数据都是每次访问时实时从 JoyAgent 接口拉取，所以不需要"清账单缓存"。

### Q6：Windows 下 PowerShell 中文乱码

每个新开的 PowerShell 会话先跑一次：

```powershell
$env:PYTHONIOENCODING='utf-8'
chcp 65001 | Out-Null
```

或者写到 `$PROFILE` 里一劳永逸。

### Q7：端口 8766 被占

```powershell
python client_dashboard.py --port 9000
```

或者先杀掉占用进程：

```powershell
Get-NetTCPConnection -LocalPort 8766 -State Listen | ForEach-Object {
  Stop-Process -Id $_.OwningProcess -Force
}
```

---

## 管理版（可选）

适合：管理员，需要把所有用户的 token 用量、京东云账单同时聚合并对账。

### 安装

```powershell
cd jd_token_bills
pip install -r requirements_monitor.txt
python -m playwright install chromium
```

### 首次登录

```powershell
# 1. 登录 JoyAgent（保存 cookie 到 ./joyagent_profile）
python joyagent_monitor.py --login

# 2.（可选）登录京东云用于拉对账数据
python joyagent_monitor.py --login-jdcloud
```

### 启动管理后台

```powershell
python web_dashboard.py
# 默认 http://127.0.0.1:8765/
```

### 历史账单导入

```powershell
# 从 JoyAgent 复制粘贴的明细（TSV）
python joyagent_monitor.py --import-bills bills_paste_2026-04.tsv

# 或者京东云下载的 CSV（gb18030 编码）
python joyagent_monitor.py --import-bills 明细账单-xxx.csv

# 或者京东云在线接口（先登录 --login-jdcloud）
python joyagent_monitor.py --import-jdcloud --month 2026-05
```

### 持续轮询

```powershell
# 每 10s 拉一次，发现差异即写入 SQLite
python joyagent_monitor.py --interval 10 --month 2026-05
```

数据会进 SQLite 库 `joyagent_usage.db`：

| 表 | 说明 |
|---|---|
| `usage_records` | 当前快照 |
| `usage_changes` | 变更日志（每次轮询发现的增量） |
| `balance_snapshots` | 余额变化 |
| `historical_bills` | 从 TSV/CSV 导入的历史账单 |
| `jdcloud_bills` | 京东云接口拉到的账单 |
| `raw_responses` | （可选 `--store-raw`）原始 JSON |

---

## 文件 / 数据安全

| 项 | 是否随仓库 |
|---|---|
| `joyagent_profile/`（登录态、cookie、本地存储） | **不**（已在 `.gitignore`） |
| `*.db`（SQLite 数据库） | **不**（已在 `.gitignore`） |
| `*.log`、`__pycache__/`、`venv/`、`.env` | **不**（已在 `.gitignore`） |
| 源代码、`*.tsv` 样例账单（已脱敏） | 是 |

切勿把 `joyagent_profile/` 提交到任何公开仓库——它包含可以直接接管你 JoyAgent / 京东云账号的 cookie。

---

## 升级 / 拉新版本

```powershell
git pull
# 如果新版本动了依赖，再跑一次：
pip install -r jd_token_bills_client/requirements.txt
# 重启服务即可
```

升级后看顶栏 build 时间戳是否变化，确认浏览器已加载新版。

---

## 卸载

直接删除 `jd_token_bill/` 文件夹即可。Playwright 下载的 Chromium 在用户目录里，可手动删除：

- Windows: `%USERPROFILE%\AppData\Local\ms-playwright\`
- macOS / Linux: `~/.cache/ms-playwright/`

---

## License

仅供研究和内部使用，与京东 / JoyAgent 官方无关，不提供任何保证。
