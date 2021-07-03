# :alarm_clock: kafka-offset-tool

A simple Go tool to reset kafka offsets.

## Requirements

- [Download](https://kafka.apache.org/downloads) kafka binaires
- Add the `bin` folder to your path
- Add your `jks` files to `/opt/secrets`
- Add `secu.build.config` and `secu.prod.config` to `/opt/secrets`

## Build

Get Go [binaries](https://golang.org/dl/) and add them to your path. Go to the project directory and run

```bash
go build
```

You can also specify the name of the executable:

```bash
go build -o kafka-offset-tool
```

If you want to build for multiple platforms, see the [documentation](https://www.digitalocean.com/community/tutorials/how-to-build-go-executables-for-multiple-platforms-on-ubuntu-16-04)

## Usage

```bash
./kafka-offset-tool
```

The executable can be anywhere on your filesystem as long as the following file `topics.json` is in the execution directory.
Make sure to set environment variables for the brokers addresses:
| Env | Value |
| ----------- | ----------- |
| BROKERS\_\<ENV\> | broker_address |

```json
{
  "env": "ENV",
  "topics": [
    {
      "topic": "topic_name",
      "group": "consumerGroup",
      "date": "2021-01-08T14:00:00.000",
      "offset": 0,
      "partitions": [0, 1, 2],
      "reset_mode": "EARLIEST"
    }
  ]
}

```

The following reset mode are available by default with this tool:

- EARLIEST: reset offsets to the minimum value
- LATEST: reset offsets to the maximum value
- OFFSET: reset offsets to the given value `offset`
- DATETIME: reset offsets to the given date `date`

`partitions` allows you to specify the partitions on which you want to apply the reset. If you want to reset offsets on all partitions, set the value to `[]`

