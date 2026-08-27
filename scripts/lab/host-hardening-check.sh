#!/usr/bin/env bash
# host-hardening-check.sh：M5.1 内部生产就绪 —— 只读安全审计（mvp-plan §9.1）。
#
# 约定：本脚本绝不写 /etc 或改任何系统状态；只输出 PASS/WARN/FAIL + 修复 runbook
# 链接（docs/runbook-hardening.md）。实验室环境（/etc 冻结）跑完审计后按需逐条
# 迁移到生产机器上实施。
set -uo pipefail

TS() { date '+%H:%M:%S'; }
say() { echo "[hardening $(TS)] $*"; }
RESULT=0
pass() { say "PASS  $1"; }
warn() { say "WARN  $1"; RESULT=0; }
fail() { say "FAIL  $1"; RESULT=1; }

say "== 只读 host hardening 审计开始 =="

# 1. ASLR / 链接保护 / 内核地址
S() { cat "/proc/sys/$1" 2>/dev/null; }
[[ "$(S kernel.randomize_va_space)" == "2" ]] && pass "ASLR full" || fail "kernel.randomize_va_space != 2"
[[ "$(S fs.protected_hardlinks)" == "1" ]] && pass "protected_hardlinks" || warn "fs.protected_hardlinks != 1"
[[ "$(S fs.protected_symlinks)" == "1" ]] && pass "protected_symlinks" || warn "fs.protected_symlinks != 1"
[[ "$(S kernel.kptr_restrict)" != "0" ]] && pass "kptr_restrict" || warn "kptr_restrict = 0（内核地址暴露给本机用户）"

# 2. SUID/setuid 二进制面
SUID_N=$(find /usr/bin /usr/sbin /bin /sbin -xdev -perm -4000 2>/dev/null | wc -l)
[[ "$SUID_N" -le 25 ]] && pass "SUID 二进制 $SUID_N 个" || warn "SUID 二进制 $SUID_N 个（建议评审必要性）"

# 3. sshd 配置（只读检查）
SSHD="/etc/ssh/sshd_config"
if [[ -f "$SSHD" ]]; then
  grep -qE "^\s*PermitRootLogin\s+no" "$SSHD" && pass "sshd PermitRootLogin no" || warn "sshd PermitRootLogin 未禁（实验室允许，生产必须 no）"
  grep -qE "^\s*PasswordAuthentication\s+no" "$SSHD" && pass "sshd 禁密码" || warn "sshd 允许密码登录（生产建议公私钥+2FA）"
else
  warn "无 sshd_config"
fi

# 4. entropy
EA=$(cat /proc/sys/kernel/random/entropy_avail 2>/dev/null || echo 256)
[[ "$EA" -ge 256 ]] && pass "entropy_avail=$EA" || warn "entropy_avail=$EA 偏低"

# 5. 明文凭证扫描（日志/历史/脚本；只统计不修改不输出内容）
SECRET_HITS=$({
  cat "$HOME/.bash_history" "$HOME/.zsh_history" 2>/dev/null
  find /tmp /var/log /var/lib/firepaas-p0 "$HOME/Learn/firepaas" -maxdepth 6 \
    \( -name '*.log' -o -name '*.txt' -o -name '*.json' -o -name '*.yaml' -o -name '*.env' \) \
    -type f -readable 2>/dev/null | head -400 | xargs -r grep -lEi \
    'fp_[a-f0-9]{40}|Bearer [A-Za-z0-9_./+=]{24,}|FIREPAAS_API_TOKEN=[^[:space:]]{12,}|master.?key[=:][^[:space:]]{12,}' 2>/dev/null | wc -l
} || echo 0)
[[ "$SECRET_HITS" == "0" ]] && pass "日志/历史未发现疑似明文凭证" || warn "$SECRET_HITS 个文件疑似含口令（人工复核；日志轮转见 runbook）"

# 6. 内核网络硬化
[[ "$(S net.ipv4.conf.all.rp_filter)" == "1" ]] && pass "rp_filter" || warn "rp_filter 未开（反向路径过滤）"
IPFWD=$(S net.ipv4.ip_forward)
[[ "$IPFWD" == "1" ]] && pass "ip_forward=1（firepaas slot 数据面必需）" || fail "ip_forward=$IPFWD（slot 后端无法工作）"

# 7. 资源上限基线（capacity model 阈值对齐）
CONN_MAX=$(cat /proc/sys/net/netfilter/nf_conntrack_max 2>/dev/null || echo N/A)
FD_MAX=$(cat /proc/sys/fs/file-max 2>/dev/null || echo N/A)
INODE_MAX=$(df -i / | awk 'NR==2{print $4}')
say "INFO  nf_conntrack_max=$CONN_MAX file_max=$FD_MAX root_inode_free=$INODE_MAX"

# 8. docker socket / 命名空间权限（实验室注意项）
if [[ -S /var/run/docker.sock ]]; then
  DOCKER_G=$(stat -c '%G' /var/run/docker.sock)
  [[ "$DOCKER_G" != "docker" ]] || warn "docker.sock 归 docker 组（成员可提权；实验室已知，生产隔离构建机）"
fi

if [[ -e /dev/kvm ]]; then
  KVM_G=$(stat -c '%G' /dev/kvm)
  [[ "$KVM_G" == "kvm" ]] && pass "/dev/kvm 归 kvm 组" || warn "/dev/kvm 组=$KVM_G"
else
  fail "/dev/kvm 不存在（firepaas 不可运行）"
fi

# 9. 越权可写敏感路径（防篡改面）
for p in /home/zty/.local/firepaas-lab/bin /etc/firepaas /var/lib/firepaas-p0; do
  [[ -e "$p" ]] || continue
  PERM=$(stat -c '%a' "$p")
  [[ "$PERM" == "7"[05][05] || "$PERM" == "750" || "$PERM" == "755" ]] && pass "$p perms=$PERM" || warn "$p perms=$PERM"
done

say "== 审计结束，结果 ${RESULT:0:1}（0=PASS，WARN 项见 runbook）=="
exit $RESULT
