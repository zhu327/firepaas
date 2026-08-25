#!/usr/bin/env bash
# 单机实验室 root 准备步骤（HITL）。用法: sudo bash scripts/lab/root-setup.sh
# 只做最小必要变更：/var/lib 数据目录、docker/kvm 组、ip_forward 确认。
# 不写 /etc、不开巨页、不动 systemd-resolved，避免影响本机 k8s。
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
  echo "ERROR: 需要 root。用法: sudo bash scripts/lab/root-setup.sh" >&2
  exit 1
fi

echo "==> 基础依赖（erofs-utils：hypeman 镜像转换必需）"
if ! command -v mkfs.erofs >/dev/null; then
  apt-get update -y
  apt-get install -y --no-install-recommends erofs-utils
else
  echo "    mkfs.erofs already present"
fi

echo "==> KVM 检查"
[[ -e /dev/kvm ]] || { echo "ERROR: /dev/kvm 不存在"; exit 1; }
echo "    /dev/kvm OK"

echo "==> hypeman 数据目录"
mkdir -p /var/lib/firepaas-p0/hypeman
chown -R root:root /var/lib/firepaas-p0
echo "    /var/lib/firepaas-p0/hypeman OK"

echo "==> ip_forward"
if [[ "$(cat /proc/sys/net/ipv4/ip_forward)" != "1" ]]; then
  sysctl -w net.ipv4.ip_forward=1
  echo 'net.ipv4.ip_forward=1' > /etc/sysctl.d/99-firepaas-lab.conf
  echo "    enabled"
else
  echo "    already enabled (k8s 已开启)"
fi

echo "==> 用户组（docker/kvm，重新登录后生效）"
TARGET_USER="${SUDO_USER:-${TARGET_USER:-zty}}"
if id -nG "$TARGET_USER" | grep -qw docker; then
  echo "    $TARGET_USER 已在 docker 组"
else
  usermod -aG docker "$TARGET_USER"
  echo "    $TARGET_USER 已加入 docker 组（需重新登录，或用 sg docker）"
fi
if id -nG "$TARGET_USER" | grep -qw kvm; then
  echo "    $TARGET_USER 已在 kvm 组"
else
  usermod -aG kvm "$TARGET_USER"
  echo "    $TARGET_USER 已加入 kvm 组（重新登录后可直接 /dev/kvm）"
fi

echo "==> 切换 Nomad 为 root 运行（raw_exec 需要）"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
"$HERE/stop.sh" >/dev/null 2>&1 || true
"$HERE/start.sh"

echo "==> 检查 Nomad"
curl -fsS http://127.0.0.1:4646/v1/status/leader || echo "    Nomad 未运行"

echo "==> 完成。下一步："
echo "    sudo bash scripts/lab/run-p0.sh            # 部署并检查 hypeman P0 job"
echo "    sudo bash scripts/lab/smoke-p0.sh          # 冒烟"
echo "    sudo bash scripts/bench-hypeman.sh cold 10 # 基准"
