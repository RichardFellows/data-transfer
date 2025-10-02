```

BenchmarkDotNet v0.13.12, Ubuntu 22.04.5 LTS (Jammy Jellyfish) WSL
Intel Core i7-8700K CPU 3.70GHz (Coffee Lake), 1 CPU, 12 logical and 6 physical cores
.NET SDK 8.0.119
  [Host]     : .NET 8.0.19 (8.0.1925.36514), X64 RyuJIT AVX2
  Job-PDYBDC : .NET 8.0.19 (8.0.1925.36514), X64 RyuJIT AVX2

InvocationCount=1  UnrollFactor=1  

```
| Method            | Mean     | Error    | StdDev   | Gen0      | Gen1      | Allocated |
|------------------ |---------:|---------:|---------:|----------:|----------:|----------:|
| Transfer_10K_Rows | 92.97 ms | 1.786 ms | 4.767 ms | 3000.0000 | 1000.0000 |  33.75 MB |
