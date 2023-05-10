# ybmetrics

This tool is used for monitoring Yugabyte DB metrics. It connects to the individual nodes[at port 9000] and
displays the change in metrics for the specified keys. By default will monitor changes to
`rows_inserted`,`db_seek` and `db_next` keys.

# Installation

```bash
pip install https://github.com/premkumr/ybmetrics/releases/download/v0.5.3/ybmetrics-0.5.3-py3-none-any.whl
```

# Usage

1. Default help

    ```bash
    usage: ybmetrics [-h] [-i INTERVAL] [--top TOP] [-v] [--no-vertical] [--full-tabletid] [-k KEYS] [--rwkeys] [--read]
                 [--write] [--txn] [--host HOSTS] [-m [{monitor,tablets,clean}]]

    Metrics Monitor

    options:
    -h, --help            show this help message and exit
    -i INTERVAL, --interval INTERVAL
                            time to wait
    --top TOP             top N tablet ids
    -v, --vertical
    --no-vertical
    --full-tabletid       print full tablet id
    -k KEYS, --keys KEYS  Key pattern(regex)
    --rwkeys              only rocks r/w keys
    --read                only rocks read key
    --write               only rocks write key
    --txn                 only txn keys
    --host HOSTS          tserver hosts (host:port[9000])
    -m [{monitor,tablets,clean}], --mode [{monitor,tablets,clean}]
                            Execution mode
    ```

1. Basic usage: Will connect to `127.0.0.{1..3}`

    ```bash
    ybmetrics
    ```

1. To monitor transaction operations,

    ```bash
    ybmetrics --txn
    ```
