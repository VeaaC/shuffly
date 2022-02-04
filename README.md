# Shuffly

[<img alt="build" src="https://img.shields.io/github/workflow/status/VeaaC/shuffly/Shuffly%20CI/main?style=for-the-badge">](https://github.com/Veaac/shuffly/actions?query=branch%3Amain)
[<img alt="docs" src="https://img.shields.io/docsrs/shuffly?style=for-the-badge">](https://crates.io/crates/shuffly)
[<img alt="package" src="https://img.shields.io/crates/v/shuffly?style=for-the-badge">](https://docs.rs/shuffly)

Increases compressability of data with fixed-sized records.

## Usage Examples

Compress a single file with shuffly and zstd

```sh
# Compress
cat my_file | shuffly -e | pzstd -o my_file.shuffly.pzstd
# Extract
cat my_file.shuffly.pzstd | pzstd -d | shuffly -d --output my_file
```

Compress a folder with tar, shuffly, and zstd

```sh
# Compress
tar -cf - my_folder | shuffly -e | pzstd -o my_folder.tar.shuffly.pzstd
# Extract
cat my_folder.tar.shuffly.pzstd | pzstd -d | shuffly -d | tar -xvf -
```

## Installation

The CLI app can be installed with [Cargo](https://doc.rust-lang.org/cargo/getting-started/installation.html):

```sh
cargo install shuffly
```

## Examples

Compressing OpenStreetMap data stored in [osmflat](https://github.com/boxdot/osmflat-rs) format is a good example for which `shuffly` can dramatically improve compression ratios.

The following table shows how the original data compresses with zstd versus shuffly combined with zstd.

Compression was done with `pzstd -4` on the [planet](https://planet.openstreetmap.org/) dataset converted to osmflat. The table below lists all files larger than 1 GB.

| File | Original Size | Compressed (zstd) | Compressed (shuffly + zstd) |
| ---- | ------------- | ----------------- | --------------------------- |
| nodes            | 103.62 GB | 45.69% | 29.79% |
| nodes_index      | 40.97 GB  | 50.17% | 25.27% |
| ids/nodes        | 34.54 GB  | 51.99% | 1.44%  |
| tags_index       | 11.95 GB  | 18.99% | 19.73% |
| ways             | 7.70 GB   | 33.71% | 7.95%  |
| ids/ways         | 3.85 GB   | 53.68% | 1.78%  |
| stringtable      | 1.92 GB   | 40.51% | 40.78% |
| tags             | 1.20 GB   | 28.36% | 24.45% |
| relation_members | 1.09 GB   | 24.53% | 27.12% |


## How it works

`Shuffly` detects fixed-sized data patterns by trying out different pattern sized between 1 to 64 bytes. For each pattern it reorderd bytes such that byte X of each record is group together, and stores deltas of these bytes instead of the original data.

The resulting data stream is much more compressible for pattern based compression algorithms like deflate /gz, zip, etc), zstd, or lzma.