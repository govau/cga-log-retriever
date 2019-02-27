# Usage

```bash
go build
./cga-log-retriever -bucket foo -match xxx,yyy -start 2018-01-01T00:00 -end 2019-01-01T00:00 > 2018.txt
```

Note, it will write cached entries to `.logsdb` in same directory, so delete that when done.
