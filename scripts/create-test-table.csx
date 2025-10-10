#!/usr/bin/env dotnet-script
/*
Creates a test Iceberg table for validation

Usage:
    dotnet script create-test-table.csx /tmp/test-warehouse test_table

Requirements:
    dotnet tool install -g dotnet-script
*/

#r "nuget: Microsoft.Extensions.Logging.Abstractions, 8.0.0"

using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using Microsoft.Extensions.Logging.Abstractions;

// Get arguments
var args = Args.ToArray();
if (args.Length != 2)
{
    Console.WriteLine("Usage: dotnet script create-test-table.csx <warehouse_path> <table_name>");
    Console.WriteLine("Example: dotnet script create-test-table.csx /tmp/test-warehouse test_table");
    return 1;
}

var warehousePath = args[0];
var tableName = args[1];

Console.WriteLine("========================================");
Console.WriteLine("Creating Test Iceberg Table");
Console.WriteLine("========================================");
Console.WriteLine($"Warehouse: {warehousePath}");
Console.WriteLine($"Table: {tableName}");
Console.WriteLine("========================================");
Console.WriteLine();

// Load assemblies (would need to reference the actual DLLs)
// This is a simplified example - in practice you'd compile a proper program
Console.WriteLine("⚠️  This script requires compilation.");
Console.WriteLine("Instead, run the test suite which creates tables:");
Console.WriteLine();
Console.WriteLine("  dotnet test tests/DataTransfer.Iceberg.Tests");
Console.WriteLine();
Console.WriteLine("Or create a simple console app to generate test tables.");
Console.WriteLine();

return 1;
