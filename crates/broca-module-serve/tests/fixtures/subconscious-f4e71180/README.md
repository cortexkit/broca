# Supervisor config-home golden

`config_home_resolution.json` is vendored byte for byte from subconscious commit
`f4e71180aa82158a03fa5dc31456ad57648b18db`, path
`crates/subc-daemon/tests/golden/config_home_resolution.json`. Its SHA-256 is
`4b28353a4f477c4ae64ba9cad981ae11583bf417267bc25626c4c5a383b3fc9d`.
The authority is `default_config_home` in `crates/subc-daemon/src/daemon_config.rs`.

Rust and Python read this table using isolated environment maps, never the host's variables.
They run `any` plus their native platform's rows. The only intentional difference is
`all_unset_falls_to_relative` (Unix) / `windows_all_unset_falls_to_relative` (Windows):
broca refuses an unknown config home rather than guessing `.config`. Configured relative
values remain honoured.

The adjacent `../path_ladders.json` pins composed broca directories, including precedence
when all Windows home variables coexist. The existing
`../cortexkit-store-types-0.2.2/data_home_resolution.json` remains the data-home authority;
the config golden must not be replaced with the data golden.
