# redshift-data-set-annotator


![Latest GitHub release](https://img.shields.io/github/release/mashiike/redshift-data-set-annotator.svg)
![Github Actions test](https://github.com/mashiike/redshift-data-set-annotator/workflows/Test/badge.svg?branch=main)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/mashiike/redshift-data-set-annotator/blob/master/LICENSE)

Annotator for QuickSight datasets with Redshift as the data source

The `redshift-data-set-annotator` is a tool to add dataset annotations based on column names, column comments, etc. for QuickSight datasets based on a single redshift Relation (table, view, etc.).

## Install 

#### Homebrew (macOS and Linux)

```console
$ brew install mashiike/tap/redshift-data-set-annotator
```

### Binary packages

[Releases](https://github.com/mashiike/redshift-data-set-annotator/releases)

## QuickStart 

1st configure for provisoned cluster
```shell
$ redshift-data-set-annotator configure                                                                                          
default profile is serverless?: (yes/no) [no]: no
Enter cluster identifier: warehouse
Enter db user: admin 
```

and execute annotate
```shell
$ redshift-data-set-annotator annotate --data-set-id <data-set-id>j
```

## Usage 

```
Usage: redshift-data-set-annotator <command>

Flags:
  -h, --help                     Show context-sensitive help.
      --aws-account-id=STRING    QuickSight aws account id
  -r, --region=STRING            AWS region ($AWS_REGION)
      --log-level="info"         output log level ($LOG_LEVEL)

Commands:
  configure
    Create a configuration file of redshift-data-set-annotator

  annotate --data-set-id=STRING
    Annotate a QuickSight dataset with Redshift as the data source

  version
    Show version

Run "redshift-data-set-annotator <command> --help" for more information on a command.
```

```
Usage: redshift-data-set-annotator annotate --data-set-id=STRING

Annotate a QuickSight dataset with Redshift as the data source

Flags:
  -h, --help                        Show context-sensitive help.
      --aws-account-id=STRING       QuickSight aws account id
  -r, --region=STRING               AWS region ($AWS_REGION)
      --log-level="info"            output log level ($LOG_LEVEL)

      --data-set-id=STRING          task ID
      --dry-run                     if true, no update data set and display plan
      --force-rename                The default is to keep any renaming that has already taken place. Enabling this option forces a name overwrite.
      --force-update-description    The default is to keep any renaming that has already taken place. Enabling this option forces a description overwrite.
```

## Column Comment 

Basically, we expect comments of the following form.
```
<name>
<description>
```

## LICENSE

MIT License

Copyright (c) 2022 IKEDA Masashi
