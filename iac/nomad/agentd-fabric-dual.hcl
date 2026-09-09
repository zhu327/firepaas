# ADR-0040 G1 spike 双节点实验室：一个 system job 两个 group，同主机
# 两个 Nomad client（scripts/lab/nomad-client2.hcl，meta.fabric_node=b）各跑
# 一个 agentd。控制面 FIREPAAS_AGENT_JOB_NAME=firepaas-agentd-dual 指向本 job。
# 与 agentd-single.hcl（单机基线）互斥使用；端口/桥/子网/data_dir 全隔离。
variable "repo_root" {
  type    = string
  default = "~/Learn/firepaas"
}

variable "lab_bin" {
  type    = string
  default = "~/.local/firepaas-lab/bin"
}

variable "agentd_binary_sha256" {
  type    = string
  default = "development"
}

job "firepaas-agentd-dual" {
  type      = "system"
  node_pool = "compute"
  priority  = 91

  # node-a（无 fabric_node 标记的单机实验室同路径）。
  group "agent" {
    constraint {
      attribute = "${meta.fabric_node}"
      operator  = "!="
      value     = "b"
    }

    network {
      port "grpc" {
        static = 5108
      }
      port "proxy" {
        static = 5107
      }
      port "metrics" {
        static = 9464
      }
    }

    service {
      name     = "firepaas-agentd"
      port     = "grpc"
      provider = "nomad"
      check {
        type     = "tcp"
        name     = "agentd-grpc"
        interval = "20s"
        timeout  = "5s"
      }
    }

    task "agentd" {
      driver = "raw_exec"

      restart {
        attempts = 5
        delay    = "5s"
        interval = "10m"
        mode     = "fail"
      }

      resources {
        memory     = 8192
        memory_max = -1
      }

      env {
        CONFIG_PATH               = "${var.repo_root}/scripts/lab/agentd.yaml"
        HYPEMAN_DOCKER_HUB_MIRROR = "docker.m.daocloud.io"
        FIREPAAS_AGENT_GRPC_PORT  = "5108"
        FIREPAAS_BUILD_SHA256     = var.agentd_binary_sha256
        FIREPAAS_AGENT_PROXY_PORT = "5107"
        FIREPAAS_AGENT_NODE_POOL  = "compute"
        FIREPAAS_AGENT_NODE_ID    = "${node.unique.id}"
        FIREPAAS_AGENT_BIND       = "0.0.0.0"
        FIREPAAS_NETWORK_BACKEND  = "ebpf"
        # ADR-0040 G1：WG mesh underlay（node-b 用 51920）。
        FIREPAAS_MESH                    = "eastwest"
        FIREPAAS_MESH_WG_PORT            = "51820"
        FIREPAAS_IMAGE_MAX_UNPACK_MIB    = "4096"
        FIREPAAS_AGENT_AUTOSTANDBY       = "true"
        FIREPAAS_AGENT_METRICS_PORT      = "9464"
        FIREPAAS_PREFETCH_DISK_WATERMARK = "0.9"
        FIREPAAS_AGENT_TLS_CERT          = "${var.repo_root}/scripts/lab/certs/agentd.crt"
        FIREPAAS_AGENT_TLS_KEY           = "${var.repo_root}/scripts/lab/certs/agentd.key"
        FIREPAAS_AGENT_TLS_CA            = "${var.repo_root}/scripts/lab/certs/ca.crt"
      }

      config {
        command = "${var.lab_bin}/agentd"
      }
    }
  }

  # node-b（meta.fabric_node=b 的第二 Nomad client）。
  group "agent-b" {
    constraint {
      attribute = "${meta.fabric_node}"
      value     = "b"
    }

    network {
      port "grpc" {
        static = 5118
      }
      port "proxy" {
        static = 5117
      }
      port "metrics" {
        static = 9474
      }
    }

    service {
      name     = "firepaas-agentd-b"
      port     = "grpc"
      provider = "nomad"
      check {
        type     = "tcp"
        name     = "agentd-grpc"
        interval = "20s"
        timeout  = "5s"
      }
    }

    task "agentd" {
      driver = "raw_exec"

      restart {
        attempts = 5
        delay    = "5s"
        interval = "10m"
        mode     = "fail"
      }

      resources {
        memory     = 8192
        memory_max = -1
      }

      env {
        CONFIG_PATH               = "${var.repo_root}/scripts/lab/agentd-b.yaml"
        HYPEMAN_DOCKER_HUB_MIRROR = "docker.m.daocloud.io"
        FIREPAAS_AGENT_GRPC_PORT  = "5118"
        FIREPAAS_BUILD_SHA256     = var.agentd_binary_sha256
        FIREPAAS_AGENT_PROXY_PORT = "5117"
        FIREPAAS_AGENT_NODE_POOL  = "compute"
        FIREPAAS_AGENT_NODE_ID    = "${node.unique.id}"
        FIREPAAS_AGENT_BIND       = "0.0.0.0"
        FIREPAAS_NETWORK_BACKEND  = "ebpf"
        # ADR-0040 G1：同主机双节点 → WG/egress 端口均与 node-a 隔离
        # （node-a：WG 51820、egress 18080/18443）；WG 端口经
        # ServiceInfo.fabric_wg_port 上报，控制面按节点组 endpoint。
        FIREPAAS_MESH          = "eastwest"
        FIREPAAS_MESH_WG_PORT  = "51920"
        FIREPAAS_MESH_WG_IFACE = "fp-wg1"
        FIREPAAS_EBPF_PIN_DIR  = "/sys/fs/bpf/firepaas-b"
        # slot 名字/veth 地址池隔离（node-a：fp/10.12.0.0/16）。
        FIREPAAS_SLOT_NAME_PREFIX        = "fpb"
        FIREPAAS_SLOT_VETH_CIDR          = "10.13.0.0/16"
        FIREPAAS_EGRESS_PROXY_PORT80     = "18180"
        FIREPAAS_EGRESS_PROXY_PORT443    = "18543"
        FIREPAAS_IMAGE_MAX_UNPACK_MIB    = "4096"
        FIREPAAS_AGENT_AUTOSTANDBY       = "true"
        FIREPAAS_AGENT_METRICS_PORT      = "9474"
        FIREPAAS_PREFETCH_DISK_WATERMARK = "0.9"
        FIREPAAS_AGENT_TLS_CERT          = "${var.repo_root}/scripts/lab/certs/agentd.crt"
        FIREPAAS_AGENT_TLS_KEY           = "${var.repo_root}/scripts/lab/certs/agentd.key"
        FIREPAAS_AGENT_TLS_CA            = "${var.repo_root}/scripts/lab/certs/ca.crt"
      }

      config {
        command = "${var.lab_bin}/agentd"
      }
    }
  }
}
