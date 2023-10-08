# holy-diver

Directories are currently not created so the `--data-dir` must already exist before running these commands:

```
Clap example that sets up holy-diver based on passed arguments
cargo run --example clap -- --data-dir ./examples/data1

Single node setup that hosts its foca endpoint at 127.0.0.1:9000 and hosts a REST endpoint at 127.0.0.1:9090
cargo run --example lone-diver

Joining node that hosts its foca endpoint at 127.0.0.1:9001 tries to join another node at 127.0.0.1:9000 and hosts a REST endpoint at 127.0.0.1:9091
cargo run --example joining-diver

target\debug\holy-diver --data-dir ./target/data
target\debug\holy-diver --announce-to 127.0.0.1:9000 --data-dir ./target/data2 --bind-address 127.0.0.1:9001 --broadcast true -p 9091
```
