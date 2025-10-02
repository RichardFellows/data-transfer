using BenchmarkDotNet.Running;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Toolchains.InProcess.Emit;

namespace DataTransfer.Benchmarks;

class Program
{
    static void Main(string[] args)
    {
        var config = DefaultConfig.Instance
            .WithOption(ConfigOptions.DisableOptimizationsValidator, true);

        // Run all benchmarks if no arguments provided
        if (args.Length == 0)
        {
            Console.WriteLine("Running all benchmark suites...");
            Console.WriteLine("To run specific benchmarks, use:");
            Console.WriteLine("  --filter *ExtractionBenchmarks*");
            Console.WriteLine("  --filter *ParquetBenchmarks*");
            Console.WriteLine("  --filter *LoadingBenchmarks*");
            Console.WriteLine("  --filter *PartitionStrategyBenchmarks*");
            Console.WriteLine();

            var summary = BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, config);
        }
        else
        {
            var summary = BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, config);
        }
    }
}
