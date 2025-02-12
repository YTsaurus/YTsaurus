# syntax=docker/dockerfile:1

FROM ubuntu:jammy AS builder

RUN <<EOF
  set -eux
  export DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC
  apt-get update
  apt-get install -y build-essential libaio-dev
EOF

WORKDIR /src

COPY . .

RUN <<EOF
  set -eux
  ./configure --dynamic-libengines
  mkdir -p /out/usr/local/share/man
  ln -s share/man /out/usr/local/man
  make install DESTDIR=/out
EOF

FROM ubuntu:jammy AS runner

RUN <<EOF
#!/bin/bash
set -eux
export DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC
apt-get update
packages=(
  curl
  dnsutils
  gdb
  iproute2
  iputils-ping
  less
  libidn11-dev
  lsb-release
  lsof
  strace
  telnet
  tini
  unzip
  zstd
  python3-pip
  python3-venv
)
apt-get install -y ${packages[@]}
EOF

RUN <<EOF
#!/bin/bash
set -eux
packages=(
  ytsaurus-yson
  ytsaurus-client
  ytsaurus-rpc-driver
)
python3 -m pip install ${packages[@]}
EOF

COPY --from=builder /out/usr/local /usr/local
COPY fio-ytsaurus.so /usr/local/lib/fio/
