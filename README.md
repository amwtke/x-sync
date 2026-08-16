# X-Sync

X-Sync 是一个基于仓库证据的自适应问答 Skill，用来持续发现并修正三类偏差：

- 人对业务、架构、代码和历史决策的理解；
- Agent 对当前仓库事实的理解；
- 仓库是否提供了足够清楚、可追溯、可验证的知识。

它不会用一个神秘的“同步值”判断谁懂整个仓库，而是针对具体任务、具体 commit 和具体题目，报告业务理解、技术机制、历史决策、非功能性要求、无提示作答、延迟保持和信心校准等维度。

## 能力

- 默认 Dialogue v2：连续、自适应、一次一问、自然结束，不绑定固定题数；
- 话题候选、自定义话题、澄清、帮助、视角切换、暂停、切换、恢复与导出；
- 旧版固定题库问答保留为显式 legacy 模式；
- 从 Spec、Story、ADR、Commit、Bug Fix、测试和核心代码生成问题；
- 从项目代码扩展到框架、MySQL、Redis、网络、OS、Docker、性能、事务、一致性、安全与可观测性；
- 终端答题与本地 HTML 答题；
- 首次运行先建立整个安全工程目录的可验证扫描清单；
- 按用户保存答题事件、证据、复习计划和多维学习画像；
- Codex 与 Claude Code 共用同一份 `SKILL.md`、运行时和数据格式；
- Python 标准库本地运行，不在运行时调用模型 API。

Codex 或 Claude Code 负责阅读仓库、生成有证据的题目和复核语义答案；本地 Python 运行时只负责校验、状态机、确定性判题、事件记录、复习调度和报告。这样同一份学习记录不会绑定某一家模型 API。

## 安装

克隆仓库后，同时安装到 Codex 和 Claude Code 的用户级 Skill 目录：

```bash
python3 skills/x-sync/scripts/install.py --host all --scope user
```

只安装一种宿主：

```bash
python3 skills/x-sync/scripts/install.py --host codex --scope user
python3 skills/x-sync/scripts/install.py --host claude --scope user
```

安装到某个项目：

```bash
python3 skills/x-sync/scripts/install.py \
  --host all --scope project --project /path/to/repository
```

安装器不会覆盖一个不由它管理的同名目录。

Codex 中使用 `$x-sync`；Claude Code 独立安装后使用 `/x-sync`。本仓库也包含 Claude Code plugin/marketplace 清单，可通过插件方式安装并使用 `/x-sync:x-sync`。

## 默认启动

在要评估的仓库中直接调用，不需要先填写配置：

```text
$x-sync
```

或在 Claude Code 中：

```text
/x-sync
```

裸调用会使用当前本地账户作为默认 learner，研究仓库并打开 Dialogue v2 本地 HTML 页面。默认行为是：

- 苏格拉底模式；
- 网页交互；
- 根据任务在业务与技术视角之间自适应；
- 一次只显示一个问题，下一问依据上一轮回答生成；
- 没有固定题数，满足话题理解门槛后自然完成；
- 可随时请求帮助、切换视角、暂停、换话题、恢复或导出；
- 从仓库证据中选择一个有边界的 onboarding 范围。

要针对当前工作目录之外的项目出题，使用 `-d` 指定目标项目：

```text
$x-sync -d /path/to/target-project
```

Claude Code 中同样适用：

```text
/x-sync -d ../target-project
```

通过 Claude plugin 安装时使用：

```text
/x-sync:x-sync -d ../target-project
```

`-d` 会贯穿扫描、证据、对话与导出全流程；问题只针对该目标项目，后续“继续”也沿用它。相对路径从宿主当前工作目录解析，Git 子目录会归一到仓库根目录，因此 `-d` 选择的是包含该目录的整个 Git worktree，并不是 monorepo 子目录过滤器；包含空格的路径需要加引号。

未完成的 v2 对话会优先恢复，不会被默认配置覆盖；旧 v1 测验不会劫持裸调用。准备第一个 v2 对话前，X-Sync 会检查整个安全工程目录并选择聚焦证据。扫描范围包含 Git 已跟踪和未忽略的未跟踪普通文件；Agent 从文档、源码、测试、配置、迁移、基础设施与历史中选择有依据的话题和问题。

“整个工程”不等于读取已知的凭据文件或第三方缓存：`.git/`、`.x-sync/`、`.env*`、常见 secret/credential/token 配置和密钥、vendor/依赖与构建产物、Git ignored 文件、二进制、超大文件、符号链接及 submodule 都不会作为扫描内容打开。首次门禁完成后，后续调用不会仅因启动 X-Sync 就重复全量扫描；仓库或任务变化时仍会按状态和证据新鲜度定向刷新。完整扫描要求至少有一个 commit 的普通 Git worktree，非 Git 目录、unborn repository 或 sparse checkout 会明确停止。

也可以手动重建清单：

```bash
python3 skills/x-sync/scripts/xsync.py scan --repo /path/to/repository --json
```

## 显式旧版测验

只有明确要求 legacy、固定题库或固定题数时才进入 v1。例如：

```text
$x-sync legacy：检查支付退款链路，常规模式，纯技术，固定 8 道题。
```

```text
/x-sync 针对最近 20 个 bug-fix commit 做一次技术复习，终端常规问答。
```

legacy 模式继续支持题库、固定 count、终端答题和旧报告；它不是默认入口。

HTML 页面保存答案后，回到 Codex 或 Claude Code 输入：

```text
继续
```

宿主 Agent 会读取 durable pending work，重新核对引用的仓库证据，并生成与本轮回答相关的下一步；不会按预设题号机械推进。

## 本地数据

学习数据默认保存在被评估仓库中：

```text
.x-sync/
  dialogue-v2/
    registry/
    sessions/
    runtime/
  repositories/<repo-id>/
    scan.json
  users/<learner>/
    profile.json
    projects/<repo-id>/
      banks/
      mastery.json
      sessions/<session-id>/        # legacy v1
        bank.json
        config.json
        state.json
        events/
        reports/
```

运行时会把 `/.x-sync/` 写入该仓库本地的 `.git/info/exclude`，不修改团队共享的 `.gitignore`。个人答案和学习画像不应提交；需要共享的题库应显式导出到团队选择的受版本控制目录。网页 Bearer token 只存在 URL fragment/浏览器 session storage 与运行进程内，不写入学习记录。

## 开发与测试

```bash
python3 -m unittest discover -s tests -p 'test_*.py' -v
python3 skills/x-sync/scripts/xsync.py doctor --repo . --json
```

核心 Skill 位于 [`skills/x-sync`](skills/x-sync)。评测方法和 JSON 协议位于其 `references/` 目录。

## 设计边界

- X-Sync 评估的是“某人在某个版本上完成某类任务的准备程度”，不是永久能力标签。
- 业务意图优先采用 Spec、Story、ADR 和验收证据，不能只从代码倒推。
- 证据冲突、题目过期或学习者提出有效反证时，应进入复核或争议状态，不能强行判错。
- 生产发布权限必须由独立工程控制决定，不能由答题结果自动授予。
