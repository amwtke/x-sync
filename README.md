# X-Sync

X-Sync 是一个围绕真实代码仓库的开放式问答 Skill。你在本地页面提问，Codex 或 Claude Code 自动读取相关证据并回答；可以持续追问、讨论设计和复盘问题。

## 默认体验

- 类似聊天工具的单一对话界面，始终保留一个输入框；
- 用户自由提问，宿主直接回答，不要求先选话题或回答 AI 的题目；
- 不知道从哪里开始时，可以点击推荐问题，也可以自己输入；
- 提交后自动调用本机已登录的 Codex／Claude Code，回复直接显示在网页，无须回终端输入“继续”；
- 连续对话会携带近期聊天记录，并在每一轮刷新仓库上下文；
- 支持停止回答、失败重试、刷新恢复，以及导出单独的 Markdown；
- 历史的苏格拉底 Dialogue v2 和固定题库 legacy 模式保留为显式入口。

Python 运行时使用标准库，负责网页服务、记录、宿主进程和回复推送。它不直接调用模型 API；语义回答由本机的 Codex／Claude Code CLI 完成，使用其已有的登录和配置。

## 安装

```bash
python3 skills/x-sync/scripts/install.py --host all --scope user
```

也可以只安装一种宿主，或安装到指定项目：

```bash
python3 skills/x-sync/scripts/install.py --host codex --scope user
python3 skills/x-sync/scripts/install.py --host claude --scope project --project /path/to/project
```

安装器不会覆盖一个不由它管理的同名目录。Claude Code 也可以通过仓库中的 plugin/marketplace 清单安装。

## 启动

在目标仓库中调用：

```text
$x-sync
```

Claude Code 中使用 `/x-sync`，插件方式使用 `/x-sync:x-sync`。默认打开自由聊天，沿用当前宿主。也可以指定另一个目标项目：

```text
$x-sync -d /path/to/target-project
/x-sync -d ../target-project
/x-sync:x-sync -d ../target-project
```

相对路径从调用时的工作目录解析；Git 子目录会归一到整个 worktree 根目录，`-d` 不是 monorepo 子目录过滤器。包含空格的路径需要加引号，后续操作沿用相同目标。

也可以直接启动，不依赖前台 Agent 一直等待：

```bash
python3 skills/x-sync/scripts/xsync.py chat serve --repo . --host codex --open
python3 skills/x-sync/scripts/xsync.py chat serve --repo . --host claude --open
```

所选宿主需要预先安装并登录。`--port 0` 自动选择本地端口；`--timeout 600` 限制单次回答的最长等待时间；`--learner` 默认使用当前本地账户。相同项目与 learner 会恢复已有自由聊天记录。

Codex 按其 JSON 消息事件推送回答；Claude Code 支持增量文字事件。页面会在宿主产生回复时更新，具体粒度取决于宿主，不保证所有宿主都逐字输出。每个问题会启动一次宿主进程，页面关闭或前台 Agent 结束不会使已保存的问题消失。

## 仓库上下文

自由聊天每轮更新仓库文件清单和 README 摘录，并让宿主只读相关源码、测试及文档。回答应引用文件路径和行号，区分实现、需求、推断与冲突。较长的聊天只携带近期完整消息，完整记录仍可导出。

仓库扫描会跳过 `.git/`、`.x-sync/`、`.env*`、凭据和密钥、依赖与构建目录、忽略文件及符号链接。浏览器里的提问用于阅读和解释项目，不授权修改代码、执行部署或发送外部消息。

原有的全量安全扫描与证据快照工具仍可显式调用：

```bash
python3 skills/x-sync/scripts/xsync.py scan --repo . --json
python3 skills/x-sync/scripts/xsync.py doctor --repo . --json
```

## 本地记录与导出

```text
.x-sync/
  chat/learner-<id>/
    session.json
    exports/x-sync-<timestamp>-<id>.md
  dialogue-v2/                       # 历史引导式对话
  repositories/<repo-id>/            # 扫描和旧证据
  users/<learner>/                   # legacy 题库、画像和复习记录
```

自由聊天记录单独保存，不会覆盖旧版对话。个人记录使用当前用户私有权限，`.x-sync/` 保持在 Git 排除范围。浏览器访问令牌只存在运行进程和浏览器中，不写入对话或导出文件。

页面右上角的“导出 MD”会下载一份独立的 UTF-8 Markdown，同时在本机的 `exports/` 中保存。导出包含用户问题和宿主回复，未完成的回复会标明状态；整个过程不调用模型。

## 显式引导和测验

明确请求“苏格拉底对话”或“Dialogue v2”时，仍可使用原有的一次一问、业务／技术视角、暂停恢复和理解门槛流程，操作见 [Dialogue v2 手册](skills/x-sync/references/dialogue-v2.md)。

明确请求 legacy、固定题库或固定题数时，使用旧版测验、学习画像和间隔复习。旧版 `start`、`answer`、`review`、`continue`、`report` 命令继续有效；这些模式都不是默认入口。

## 开发与测试

```bash
python3 -m unittest tests.test_xsync_chat -v
python3 -m unittest discover -s tests -p 'test_*.py'
node --check skills/x-sync/assets/chat.js
```

核心代码：

- [开放式聊天运行时](skills/x-sync/scripts/xsync_chat/)
- [聊天页面](skills/x-sync/assets/chat.html)
- [技能入口](skills/x-sync/SKILL.md)
- [自由聊天运行说明](skills/x-sync/references/open-chat.md)
- [历史 Dialogue v2 运行时](skills/x-sync/scripts/xsync_v2/)
