# target-storeforce

A [Singer](https://singer.io) target that egresses data via CSV+SFTP.
Ultimately the destination is configured via a Singer config.json, although
currently this is only targeted to outbound to Storeforce.

## Usage

To use, 

1. Install Python 3 (see [.python-version](./.python-version)).
2. Install the application and its dependencies.
3. Create a `config.json` file which will configure the component.  An example
follows, with descriptions of the configuration fields
[following](#configuration-fields).
4. Run the target.  Usually this involves also running a Singer tap and piping
the tap's output to the target:

```sh
tap-snowflake | target-storeforce --config ./config.json
```

### Example `config.json`

```json
{
  "delimiter" : ",",
  "destination_path" : "/destination/",
  "file_mapping": {
    "stream_name": "DATA_NONPROD-DA3-V_STOREFORCE_POS",
    "file_name": "pos.txt",
    "output_map": [
       "STORE_CODE",
       "TRANSACTION_DATE",
       "TRANSACTION_SLOT",
       "SALE_TRANS",
       "SALE_VALUE",
       "SALE_UNITS",
       "REFUND_TRANS",
       "REFUND_VALUE",
       "REFUND_UNITS"
    ]
  },
  "header" : true,
  "host" : "sftp_server_address",
  "password" : "sftp_password",
  "port" : 22,
  "private_key_file" : "/location/id_rsa",
  "quotechar" : "'",
  "username" : "user",
  "timestamp_file" : true
}
```

### Configuration fields

- `delimiter`: The field delimiter used in the output file.  Note that
  carriage-returns are used for the record delimiter. Defaults to `,`.
- `destination_path`: The destination directory on the remote SFTP server,
  relative to the SFTP user's mount point. In the above sample, if the `user`
  SFTP user's SFTP mount point is `/home/user`, then the destination directory's
  fully-qualified path would be `/home/user/destination`.
- `file_mapping`: Configures how to map `RECORD` message fields to the tabular
  format:
  - `stream_name`: The data stream to pull records from (as indicated by the
    `stream` attribute in the `RECORD` messages.
  - `file_name`: Overrides the filename to be used when uploading the file, in
    conjunction with the `destination_path`.  Using the above sample, the file
    would be uploaded to the SFTP host at `/home/user/destination/pos.txt`.
    **IMPORTANT NOTE: The current vendor, Storeforce, requires a singular,
    previously-defined filename, that gets overwritten on every execution.
    Currently the value for this is `pos.txt` (i.e. `"file_name": "pos.txt"`).
    Unless circumstances change, using any value other than `pos.txt` will break
    the integration.  You have been warned.**
  - `output_map`: Indicates the order of columns in the output file. Each
    message is parsed and the fields extracted; they are then written to the
    tabular data in the order indicated by `output_map`.  In the above sample,
    the first column of each record would be the value of the `STORE_CODE`
    message property; the second column would be `TRANSACTION_DATE`, and so on.
- `header`: Toggles to write a header line to the output file. Defaults to
  `false`.
- `host`: The SFTP host to upload to.
- `password`: The SFTP user's password.  Either `password` or `private_key_file`
  can be used, although `private_key_file` is strongly recommended.
- `port`: The SFTP port to connect to (defaults to port 22).
- `private_key_file`: The path, local to the machine the app is running on, to
  the RSA key file that should be used to authenticate to the SFTP host.  Either 
  `password` or `private_key_file` can be used, although `private_key_file` is
  strongly recommended.
- `quotechar`: The character to use for strings containing embedded
  `delimeter`s. Defaults to `'`.
- `username`: The SFTP username to use.
- `timestamp_file`: Toggles whether or not to mark the staged file (not the
  final, uploaded file) using a timestamp.  Defaults to `true`.

## Development

1. Install Python [3.*](./.python-version) as you see fit (I like `asdf` but
whatever tickles your fancy).
2. Run `make setup`.  This does the following (feel free to execute these steps
yourself as an alternative):
  1. Installs `poetry`.
  2. Installs `direnv`.
  3. Runs `poetry` to pull dependencies.
  4. Runs `pytest`.
  5. Calculates and reports test coverage.
  6. Lints the code.

### Configuration

The project is built and managed by [`poetry`](https://python-poetry.org), which
uses [`pyproject.toml`](./pyproject.toml) for just about everything.  It's [not
perfect](#distribution), but it seems like the best option, at least right now.

### Notes

#### Guidelines

The project has been set up to enforce good practices, including linted code and
test coverage.  As currently configured, the project will fail at CircleCI if
tests fail, test coverage drops below 70%, or the linter produces any errors.
`make build` will check all of these, and each one can be run independently
using `make test`, `make coverage`, or `make lint`, respectively. Circle will
run each as an independent step for troubleshooting and traceability purposes.

#### Versioning

Use semantic versioning.  Use Git tags to tag releases; e.g. `git tag -a 0.1.0
-m 0.1.0 && git push --tags`.

#### Distribution

The project is built and managed by `poetry`, which as of this writing shows a
lot of promise but is a bit buggy.  Namely, private package hosting and
retrieval (e.g.  Gemfury) is quite wonky, so for the time being the release
target `make dist` has been commented.

## Notes

The pattern being used here is in 3 parts: the entrypoint, the target, and the
generator.

### The entrypoint

This is marked as the package executable under the `[tools.poetry.scripts]`
directive in [the project defintion](./pyproject.toml).  In other words, `poetry
run target-storeforce` (or the equivalent) kicks this off.  It simply parses the
standard Singer configuration file (see [below](#configuration-file)),
instantiates a [`Target`](#the-target), and then calls `Target#run`, passing in
`stdin` and `stdout`.

### The target

The `Target`'s responsibility is to manage the application's lifecycle.  In the
case of this application, this entails 

1. performing some setup,
2. initiating [the generator](#the-generator) to build a CSV file,
3. SFTP-ing the file (using the
[`bonobos-singer-support`](https://github.com/bonobos/bonobos-singer-support)
package),
4. and finally emtting state to the output stream (i.e. `stdout`) for Singer.


### The generator

The generator's responsibility is to consume messages from the input stream
(i.e. `stdin`) and build a data file by handling the various message type(s)
appropriately.  Currently this is implemented as a simple function
(`target_storeforce.generator:generate_csv_from_messages`), but this may not
always be the case.  Note also that the module itself has some cruft, left over
from the prior implementation in `target-sftp`; namely
`target_storeforce.generator:output_state`,
`target_storeforce.generator:persist_messages_csv` and
`target_storeforce.generator:send_file`.  These have been deprecated and should
eventually be removed for clarity.

