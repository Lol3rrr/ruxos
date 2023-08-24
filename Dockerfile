FROM rust:1.71 as builder

RUN mkdir /ruxos
WORKDIR /ruxos

COPY . .

RUN rustup toolchain install nightly
RUN rustup default nightly

RUN cargo build --release --example lin-kv

FROM ubuntu:22.04

RUN mkdir /ruxos
RUN mkdir /maelstrom

COPY --from=builder /ruxos/target/release/examples/lin-kv /ruxos/lin-kv

RUN apt update

# -----
# Installing Maelstrom
# -----
# Dependencies
RUN apt install openjdk-17-jdk -y
RUN apt install graphviz -y
RUN apt install gnuplot -y

# Leiningen
RUN apt install curl -y
RUN curl -o /usr/local/bin/lein https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein
RUN chmod a+x /usr/local/bin/lein
RUN lein

# Maelstrom repo
RUN apt install git -y
RUN git clone https://github.com/jepsen-io/maelstrom.git /maelstrom

WORKDIR /maelstrom

COPY ./test.sh ./test.sh

CMD ["/bin/bash", "test.sh"]
