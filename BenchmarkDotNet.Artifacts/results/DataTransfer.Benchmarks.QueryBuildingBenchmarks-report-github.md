```

BenchmarkDotNet v0.13.12, Ubuntu 22.04.5 LTS (Jammy Jellyfish) WSL
Intel Core i7-8700K CPU 3.70GHz (Coffee Lake), 1 CPU, 12 logical and 6 physical cores
.NET SDK 8.0.119
  [Host]     : .NET 8.0.19 (8.0.1925.36514), X64 RyuJIT AVX2
  DefaultJob : .NET 8.0.19 (8.0.1925.36514), X64 RyuJIT AVX2


```
| Method                        | Mean     | Error   | StdDev  | Gen0   | Gen1   | Allocated |
|------------------------------ |---------:|--------:|--------:|-------:|-------:|----------:|
| BuildQuery_Static             | 122.0 ns | 1.62 ns | 1.51 ns | 0.0713 |      - |     448 B |
| BuildQuery_DatePartitioned    | 344.1 ns | 3.55 ns | 2.97 ns | 0.1683 |      - |    1056 B |
| BuildQuery_IntDatePartitioned | 415.7 ns | 8.09 ns | 8.99 ns | 0.1836 | 0.0005 |    1152 B |
| BuildQuery_Scd2               | 438.8 ns | 6.69 ns | 5.93 ns | 0.2127 | 0.0005 |    1336 B |
