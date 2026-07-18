# BatchWizard

BatchWizard is a durable CLI control plane for asynchronous LLM work. Submit OpenAI Batch API jobs, exit safely, and return later to inspect their remote state and collect their result and error artifacts. Its provider-neutral manifest and orchestration layer are designed to support additional batch providers without flattening their native request formats.

![image](https://github.com/user-attachments/assets/8084afbd-fd05-43b3-b57c-2ea1eb70a457)

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Commands](#commands)
- [Features](#features)
- [Contributing](#contributing)
- [License](#license)

## Installation

You can install BatchWizard using `pipx` for an isolated environment or directly via `pip`.

### Using pipx (recommended)

```bash
pipx install batchwizard
```

### Using pip

```bash
pip install batchwizard
```

Ensure you have `pipx` or `pip` installed on your system. For `pipx`, you can follow the installation instructions [here](https://pipx.pypa.io/stable/installation/).

## Usage

BatchWizard provides a command-line interface (CLI) for managing batch jobs. Here are some example commands:

### Process Batch Jobs

To process input files or directories:

```bash
batchwizard process <input_paths>... [--output-directory OUTPUT_DIR] [--max-concurrent-jobs NUM] [--check-interval SECONDS]
```

You can provide multiple input paths, which can be individual JSONL files or directories containing JSONL files.

#### Example with Sample Input

Let's say you have a file named `batchinput.jsonl` with the following content:

```jsonl
{"custom_id": "request-1", "method": "POST", "url": "/v1/chat/completions", "body": {"model": "gpt-4o-mini", "messages": [{"role": "system", "content": "You are a helpful assistant."},{"role": "user", "content": "Hello world!"}],"max_tokens": 1000}}
{"custom_id": "request-2", "method": "POST", "url": "/v1/chat/completions", "body": {"model": "gpt-4o-mini", "messages": [{"role": "system", "content": "You are an unhelpful assistant."},{"role": "user", "content": "Hello world!"}],"max_tokens": 1000}}
```

For the Responses API, use `/v1/responses` in both the JSONL request and the
submission endpoint:

```jsonl
{"custom_id":"request-1","method":"POST","url":"/v1/responses","body":{"model":"gpt-5.4","input":"Classify this support ticket as billing, technical, or other."}}
```

```bash
batchwizard submit batchinput.jsonl --endpoint /v1/responses
```

To process this file using BatchWizard:

1. First, ensure your OpenAI API key is set:
   ```bash
   batchwizard configure --set-key YOUR_API_KEY
   ```
2. Then, run the process command:
   ```bash
   batchwizard process /path/to/batchinput.jsonl --output-directory /path/to/output
   ```
   This command will:
   - Upload the `batchinput.jsonl` file to OpenAI
   - Create a batch job
   - Monitor the job status
   - Download the results to the specified output directory when complete

You can also process multiple files or directories:

```bash
batchwizard process /path/to/file1.jsonl /path/to/directory_with_jsonl_files /path/to/file2.jsonl
```

### Submit Without Blocking (fire and forget)

`process` blocks until every batch completes — which can take up to 24 hours. To submit and get your terminal back:

```bash
batchwizard submit /path/to/inputs/           # or: batchwizard process ... --submit-only
```

Submitted jobs are recorded in a local manifest (SQLite, in the BatchWizard config directory). Later—even after a reboot—reattach with:

```bash
batchwizard watch [--output-directory OUTPUT_DIR]
```

`watch` picks up every actionable job from the manifest. It polls active remote
jobs and retries artifact collection for terminal jobs whose previous download
failed.

### Check Tracked Jobs

```bash
batchwizard status [--all]
```

Shows actionable jobs from the local manifest (`--all` includes fully collected
ones). Remote lifecycle, request outcomes, local collection state, and local
connectivity errors are reported separately.

### Durable Job Lifecycle

BatchWizard never marks a provider batch as failed merely because the local
machine lost contact with it. It also does not treat provider completion as proof
that result files were collected successfully. Failed downloads remain actionable
and are retried on the next `watch`.

See [Job lifecycle](docs/job-lifecycle.md) for the state model, recovery invariants,
provider contract, and manifest migration behavior.

### Failure Reasons and Error Files

When a batch fails, BatchWizard surfaces the provider's actual error (e.g. `insufficient_funds`, `invalid_request` with the offending line). When individual requests inside a batch fail, the per-request error file is downloaded alongside the results as `<batch_id>_errors.jsonl`.

### Batch Endpoints

By default requests go to `/v1/chat/completions` for backward compatibility. Use
`--endpoint` on `process`/`submit` for `/v1/responses`, `/v1/embeddings`,
`/v1/completions`, `/v1/moderations`, `/v1/images/generations`,
`/v1/images/edits`, or `/v1/videos`. Every line's `url` must match the selected
batch endpoint.

### List Recent Jobs

To list recent batch jobs from the provider:

```bash
batchwizard list-jobs [--limit NUM]
```

### Cancel a Job

To cancel a specific batch job:

```bash
batchwizard cancel <job_id>
```

### Download Job Results

To download results for a completed batch job:

```bash
batchwizard download <job_id> [--output-directory OUTPUT_DIR]
```

This downloads the results file and, if any requests failed, the per-request error file.

## Configuration

### Setting up the OpenAI API Key

To set the OpenAI API key:

```bash
batchwizard configure --set-key YOUR_API_KEY
```

### Show Current Configuration

To show the current configuration:

```bash
batchwizard configure --show
```

### Reset Configuration

To reset the configuration to default values:

```bash
batchwizard configure --reset
```

## Commands

BatchWizard supports the following commands:

- `process`: Submit batch jobs and wait for completion (add `--submit-only` to return immediately).
- `submit`: Submit batch jobs and exit; jobs are tracked in the local manifest.
- `watch`: Advance active jobs and retry uncollected terminal artifacts.
- `status`: Show jobs tracked in the local manifest.
- `configure`: Manage BatchWizard configuration.
- `list-jobs`: List recent batch jobs from the provider.
- `cancel`: Cancel a specific batch job.
- `download`: Download results (and error file) for a batch job.

For detailed information on each command, use the `--help` option:

```bash
batchwizard <command> --help
```

## Features

- **Flexible Input**: Process individual JSONL files or entire directories containing JSONL files.
- **Asynchronous Processing**: Efficiently handle multiple batch jobs concurrently.
- **Rich UI**: Display progress and job status using a rich, interactive interface.
- **Flexible Configuration**: Easily manage API keys and other settings.
- **Job Management**: List, cancel, and download results for batch jobs.
- **Durable Recovery**: Preserve remote truth across local connectivity and artifact-download failures.
- **Responses API**: Submit modern `/v1/responses` JSONL batches alongside other OpenAI batch endpoints.

## Contributing

We welcome contributions to BatchWizard! To contribute, follow these steps:

1. Fork the repository.
2. Create a new branch: `git checkout -b feature/your-feature-name`.
3. Make your changes and commit them: `git commit -m 'Add some feature'`.
4. Push to the branch: `git push origin feature/your-feature-name`.
5. Open a pull request.

### Running Tests

To run tests, use `pytest`:

```bash
uv run pytest tests/
```

Ensure your code passes all tests and meets the coding standards before opening a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contact

For any questions or feedback, feel free to open an issue on the [GitHub repository](https://github.com/cmakafui/batchwizard).
