using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Xunit;

namespace DataTransfer.Web.Tests;

/// <summary>
/// xUnit CollectionFixture that starts the web application once before all tests run
/// and ensures it's available at http://localhost:5000 for Playwright tests
/// </summary>
public class WebApplicationFixture : IAsyncLifetime
{
    private Process? _webProcess;
    private const string ProjectPath = "src/DataTransfer.Web";
    private const string BaseUrl = "http://localhost:5000";
    private const int ServerPort = 5000;

    public string Url => BaseUrl;

    public async Task InitializeAsync()
    {
        // Check if port is already in use
        if (IsPortInUse(ServerPort))
        {
            Console.WriteLine($"⚠️  Port {ServerPort} already in use - assuming web server is running");
            return;
        }

        Console.WriteLine($"Starting web application at {BaseUrl}...");

        var startInfo = new ProcessStartInfo
        {
            FileName = "dotnet",
            Arguments = $"run --project {ProjectPath} --urls {BaseUrl}",
            WorkingDirectory = "/home/richard/sonnet45",
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true
        };

        _webProcess = new Process { StartInfo = startInfo };

        // Capture output for debugging
        _webProcess.OutputDataReceived += (sender, e) =>
        {
            if (!string.IsNullOrEmpty(e.Data))
            {
                Console.WriteLine($"[WebApp] {e.Data}");
            }
        };

        _webProcess.ErrorDataReceived += (sender, e) =>
        {
            if (!string.IsNullOrEmpty(e.Data))
            {
                Console.Error.WriteLine($"[WebApp ERROR] {e.Data}");
            }
        };

        _webProcess.Start();
        _webProcess.BeginOutputReadLine();
        _webProcess.BeginErrorReadLine();

        // Wait for server to be ready
        var ready = await WaitForServerReady(BaseUrl, TimeSpan.FromSeconds(30));

        if (!ready)
        {
            _webProcess?.Kill(true);
            throw new InvalidOperationException($"Web server failed to start at {BaseUrl} within 30 seconds");
        }

        Console.WriteLine($"✓ Web application ready at {BaseUrl}");
    }

    public Task DisposeAsync()
    {
        if (_webProcess != null && !_webProcess.HasExited)
        {
            Console.WriteLine("Shutting down web application...");
            _webProcess.Kill(true);
            _webProcess.WaitForExit(5000);
            _webProcess.Dispose();
        }

        return Task.CompletedTask;
    }

    private static bool IsPortInUse(int port)
    {
        try
        {
            using var client = new TcpClient();
            client.Connect("localhost", port);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static async Task<bool> WaitForServerReady(string url, TimeSpan timeout)
    {
        using var httpClient = new HttpClient();
        var deadline = DateTime.UtcNow.Add(timeout);

        while (DateTime.UtcNow < deadline)
        {
            try
            {
                var response = await httpClient.GetAsync(url);
                if (response.StatusCode == HttpStatusCode.OK)
                {
                    return true;
                }
            }
            catch
            {
                // Server not ready yet, continue waiting
            }

            await Task.Delay(500);
        }

        return false;
    }
}

/// <summary>
/// Collection definition for web UI tests requiring a running server
/// </summary>
[CollectionDefinition("WebApplication")]
public class WebApplicationCollection : ICollectionFixture<WebApplicationFixture>
{
    // This class is never instantiated - it's just a marker for xUnit
}
