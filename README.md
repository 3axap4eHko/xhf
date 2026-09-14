# xhf

`xhf` is a standalone Rust CLI for searching and downloading content from the Hugging Face Hub. It does not require Python or a Python package environment.

## Install

Install from crates.io:

```bash
cargo install xhf
```

Prebuilt archives and SHA-256 checksum files are also available from GitHub Releases for Linux, macOS, and Windows.

## Search

```bash
xhf search users QUERY
xhf search orgs QUERY
xhf search repos QUERY
xhf search repos QUERY --owner OWNER
xhf search repos QUERY --type model
xhf search repos QUERY --type dataset
xhf search repos QUERY --type space
```

Search commands accept `--limit N` and `--json`.

## Repositories

List repositories owned by a user or organization:

```bash
xhf repo list OWNER
xhf repo list OWNER --type dataset
```

List files and directories in a repository:

```bash
xhf repo tree OWNER/REPO
xhf repo tree OWNER/REPO 'models/**'
xhf repo tree OWNER/REPO --json
```

Write one file to standard output:

```bash
xhf repo cat OWNER/REPO path/to/file
```

## Download

Downloads are written to the current directory unless `--dir` is specified.

Download one file by exact path:

```bash
xhf download OWNER/REPO path/to/file
```

Download files selected by repository-relative globs:

```bash
xhf download OWNER/REPO 'weights/*.safetensors' 'configs/**'
```

Download a directory and its descendants:

```bash
xhf download OWNER/REPO tokenizer/
```

Exclude matching files by prefixing a quoted pattern with `!`:

```bash
xhf download OWNER/REPO '**/*.json' '!tests/**'
```

Use `--type dataset` or `--type space` for non-model repositories. Use `--revision` to select a branch, tag, or commit.

### Parallel and resumable transfers

`--jobs N` limits the total number of simultaneous HTTP transfers. A single large file is divided into byte ranges so all available jobs can download different parts concurrently. Multiple selected files share the same limit.

```bash
xhf download Barding-Defense/Qwen3.8-27B-huihui-abliterated-NVFP4-NInfer \
  qwen3_8_27b_huihui_abliterated_nvfp4.ninfer \
  --jobs 5
```

Interrupted downloads retain a hidden `.part` file and versioned `.meta` checkpoint beside the destination. Running the same command continues verified unfinished ranges. Missing, invalid, or stale metadata restarts the file from byte zero. Successful completion installs the destination and removes the sidecars.

Use `--force` to replace existing regular files and `--dry-run` to print selected repository paths without writing files.

## Authentication

```bash
xhf auth login
xhf auth status
xhf auth logout
```

`HF_TOKEN` takes precedence over the stored token. Authentication is required for private repositories and gated repositories granted to the token owner.

## License

MIT
