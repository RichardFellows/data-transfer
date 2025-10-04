using System.Text;
using System.Text.Json;

namespace DataTransfer.Console.Tests;

/// <summary>
/// Generates an HTML report from console test captures
/// Similar to ScreenshotReportGenerator but for console output
/// </summary>
public class ConsoleReportGenerator
{
    private readonly string _outputDirectory;
    private readonly string _metadataFile;
    private readonly string _reportPath;

    public ConsoleReportGenerator(
        string outputDirectory = "test-results/console-output",
        string reportPath = "test-results/ConsoleTestReport.html")
    {
        _outputDirectory = outputDirectory;
        _metadataFile = Path.Combine(outputDirectory, "captures.json");
        _reportPath = reportPath;
    }

    public async Task GenerateReportAsync()
    {
        if (!File.Exists(_metadataFile))
        {
            System.Console.WriteLine($"Metadata file not found: {_metadataFile}");
            return;
        }

        var json = await File.ReadAllTextAsync(_metadataFile);
        var captures = JsonSerializer.Deserialize<List<ConsoleOutputCapture>>(json);

        if (captures == null || !captures.Any())
        {
            System.Console.WriteLine("No console captures found to generate report");
            return;
        }

        var html = GenerateHtml(captures);
        var reportDir = Path.GetDirectoryName(_reportPath);
        if (!string.IsNullOrEmpty(reportDir))
        {
            Directory.CreateDirectory(reportDir);
        }

        await File.WriteAllTextAsync(_reportPath, html);
        System.Console.WriteLine($"Console test report generated: {Path.GetFullPath(_reportPath)}");
        System.Console.WriteLine($"Total captures: {captures.Count}");
    }

    private string GenerateHtml(List<ConsoleOutputCapture> captures)
    {
        var sb = new StringBuilder();

        sb.AppendLine("<!DOCTYPE html>");
        sb.AppendLine("<html lang='en'>");
        sb.AppendLine("<head>");
        sb.AppendLine("    <meta charset='UTF-8'>");
        sb.AppendLine("    <meta name='viewport' content='width=device-width, initial-scale=1.0'>");
        sb.AppendLine("    <title>DataTransfer Console - Test Report</title>");
        sb.AppendLine("    <style>");
        sb.AppendLine("        * { margin: 0; padding: 0; box-sizing: border-box; }");
        sb.AppendLine("        body { font-family: 'Consolas', 'Monaco', 'Courier New', monospace; background: #1e1e1e; color: #d4d4d4; padding: 20px; }");
        sb.AppendLine("        .container { max-width: 1600px; margin: 0 auto; background: #252526; padding: 40px; box-shadow: 0 4px 16px rgba(0,0,0,0.3); }");
        sb.AppendLine("        h1 { color: #4ec9b0; margin-bottom: 10px; font-size: 2.5em; }");
        sb.AppendLine("        .subtitle { color: #9cdcfe; margin-bottom: 30px; font-size: 1.1em; }");
        sb.AppendLine("        .meta { background: #2d2d30; padding: 20px; border-radius: 8px; margin-bottom: 40px; border-left: 4px solid #007acc; }");
        sb.AppendLine("        .meta p { margin: 8px 0; color: #d4d4d4; }");
        sb.AppendLine("        .meta strong { color: #dcdcaa; }");
        sb.AppendLine("        .summary-stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 40px; }");
        sb.AppendLine("        .stat-card { background: #2d2d30; padding: 20px; border-radius: 8px; border-left: 4px solid #007acc; }");
        sb.AppendLine("        .stat-card.success { border-left-color: #4ec9b0; }");
        sb.AppendLine("        .stat-card.failure { border-left-color: #f48771; }");
        sb.AppendLine("        .stat-card h3 { color: #9cdcfe; font-size: 0.9em; margin-bottom: 8px; }");
        sb.AppendLine("        .stat-card .value { color: #d7ba7d; font-size: 2em; font-weight: bold; }");
        sb.AppendLine("        .test-section { margin-bottom: 60px; }");
        sb.AppendLine("        .test-section h2 { color: #4ec9b0; margin-bottom: 20px; padding-bottom: 15px; border-bottom: 2px solid #007acc; }");
        sb.AppendLine("        .capture-item { background: #1e1e1e; border: 1px solid #3e3e42; border-radius: 8px; margin-bottom: 30px; overflow: hidden; }");
        sb.AppendLine("        .capture-header { background: #2d2d30; padding: 15px 20px; border-bottom: 1px solid #3e3e42; }");
        sb.AppendLine("        .capture-header h3 { color: #dcdcaa; font-size: 1.1em; margin-bottom: 8px; }");
        sb.AppendLine("        .capture-meta { display: flex; gap: 25px; flex-wrap: wrap; font-size: 0.9em; }");
        sb.AppendLine("        .capture-meta span { color: #858585; }");
        sb.AppendLine("        .capture-meta .label { color: #9cdcfe; }");
        sb.AppendLine("        .capture-meta .status-pass { color: #4ec9b0; font-weight: bold; }");
        sb.AppendLine("        .capture-meta .status-fail { color: #f48771; font-weight: bold; }");
        sb.AppendLine("        .command-line { background: #1e1e1e; padding: 15px 20px; border-bottom: 1px solid #3e3e42; }");
        sb.AppendLine("        .command-line code { color: #ce9178; font-family: 'Consolas', 'Monaco', monospace; }");
        sb.AppendLine("        .output-container { display: grid; grid-template-columns: 1fr; gap: 0; }");
        sb.AppendLine("        .output-section { padding: 0; }");
        sb.AppendLine("        .output-header { background: #2d2d30; padding: 10px 20px; color: #9cdcfe; font-weight: bold; border-bottom: 1px solid #3e3e42; font-size: 0.9em; }");
        sb.AppendLine("        .output-content { padding: 20px; background: #1e1e1e; max-height: 400px; overflow-y: auto; }");
        sb.AppendLine("        .output-content pre { color: #d4d4d4; font-size: 0.9em; line-height: 1.6; white-space: pre-wrap; word-wrap: break-word; }");
        sb.AppendLine("        .output-content.empty { color: #858585; font-style: italic; }");
        sb.AppendLine("        .toggle-btn { background: #007acc; color: white; border: none; padding: 8px 16px; border-radius: 4px; cursor: pointer; font-size: 0.85em; margin-top: 10px; }");
        sb.AppendLine("        .toggle-btn:hover { background: #005a9e; }");
        sb.AppendLine("        .collapsible { display: none; }");
        sb.AppendLine("        .collapsible.active { display: block; }");
        sb.AppendLine("        footer { margin-top: 60px; text-align: center; color: #858585; font-size: 0.9em; padding-top: 20px; border-top: 1px solid #3e3e42; }");
        sb.AppendLine("    </style>");
        sb.AppendLine("</head>");
        sb.AppendLine("<body>");
        sb.AppendLine("    <div class='container'>");
        sb.AppendLine("        <h1>⚡ DataTransfer Console</h1>");
        sb.AppendLine("        <p class='subtitle'>Integration Test Report</p>");

        // Calculate summary stats
        var totalTests = captures.Count;
        var passedTests = captures.Count(c => c.Success);
        var failedTests = totalTests - passedTests;
        var avgDuration = captures.Average(c => c.Duration.TotalMilliseconds);

        sb.AppendLine("        <div class='summary-stats'>");
        sb.AppendLine($"            <div class='stat-card'>");
        sb.AppendLine($"                <h3>Total Tests</h3>");
        sb.AppendLine($"                <div class='value'>{totalTests}</div>");
        sb.AppendLine($"            </div>");
        sb.AppendLine($"            <div class='stat-card success'>");
        sb.AppendLine($"                <h3>Passed</h3>");
        sb.AppendLine($"                <div class='value'>{passedTests}</div>");
        sb.AppendLine($"            </div>");
        sb.AppendLine($"            <div class='stat-card failure'>");
        sb.AppendLine($"                <h3>Failed</h3>");
        sb.AppendLine($"                <div class='value'>{failedTests}</div>");
        sb.AppendLine($"            </div>");
        sb.AppendLine($"            <div class='stat-card'>");
        sb.AppendLine($"                <h3>Avg Duration</h3>");
        sb.AppendLine($"                <div class='value'>{avgDuration:F0}<span style='font-size:0.5em'>ms</span></div>");
        sb.AppendLine($"            </div>");
        sb.AppendLine("        </div>");

        sb.AppendLine("        <div class='meta'>");
        sb.AppendLine($"            <p><strong>Generated:</strong> {DateTime.Now:yyyy-MM-dd HH:mm:ss}</p>");
        sb.AppendLine($"            <p><strong>Pass Rate:</strong> {(passedTests * 100.0 / totalTests):F1}%</p>");
        sb.AppendLine("            <p><strong>Purpose:</strong> Verify console app behavior, argument parsing, and output validation</p>");
        sb.AppendLine("        </div>");

        // Group captures by test name
        var groups = captures.GroupBy(c => c.TestName);

        foreach (var group in groups)
        {
            sb.AppendLine($"        <div class='test-section'>");
            sb.AppendLine($"            <h2>{FormatTestName(group.Key)}</h2>");

            foreach (var capture in group)
            {
                var statusClass = capture.Success ? "status-pass" : "status-fail";

                sb.AppendLine($"            <div class='capture-item'>");
                sb.AppendLine($"                <div class='capture-header'>");
                sb.AppendLine($"                    <h3>{FormatStepName(capture.StepName)}</h3>");
                sb.AppendLine($"                    <div class='capture-meta'>");
                sb.AppendLine($"                        <span><span class='label'>Status:</span> <span class='{statusClass}'>{capture.Status}</span></span>");
                sb.AppendLine($"                        <span><span class='label'>Exit Code:</span> {capture.ExitCode}</span>");
                sb.AppendLine($"                        <span><span class='label'>Duration:</span> {capture.Duration.TotalMilliseconds:F2}ms</span>");
                sb.AppendLine($"                        <span><span class='label'>Time:</span> {capture.Timestamp:HH:mm:ss}</span>");
                sb.AppendLine($"                    </div>");
                sb.AppendLine($"                </div>");
                sb.AppendLine($"                <div class='command-line'>");
                sb.AppendLine($"                    <code>$ {EscapeHtml(capture.Command)} {EscapeHtml(capture.Arguments)}</code>");
                sb.AppendLine($"                </div>");
                sb.AppendLine($"                <div class='output-container'>");

                // Standard Output
                sb.AppendLine($"                    <div class='output-section'>");
                sb.AppendLine($"                        <div class='output-header'>STDOUT</div>");
                if (string.IsNullOrWhiteSpace(capture.StandardOutput))
                {
                    sb.AppendLine($"                        <div class='output-content empty'>(no output)</div>");
                }
                else
                {
                    sb.AppendLine($"                        <div class='output-content'>");
                    sb.AppendLine($"                            <pre>{EscapeHtml(capture.StandardOutput)}</pre>");
                    sb.AppendLine($"                        </div>");
                }
                sb.AppendLine($"                    </div>");

                // Standard Error (only show if not empty)
                if (!string.IsNullOrWhiteSpace(capture.StandardError))
                {
                    sb.AppendLine($"                    <div class='output-section'>");
                    sb.AppendLine($"                        <div class='output-header'>STDERR</div>");
                    sb.AppendLine($"                        <div class='output-content'>");
                    sb.AppendLine($"                            <pre>{EscapeHtml(capture.StandardError)}</pre>");
                    sb.AppendLine($"                        </div>");
                    sb.AppendLine($"                    </div>");
                }

                sb.AppendLine($"                </div>");
                sb.AppendLine($"            </div>");
            }

            sb.AppendLine($"        </div>");
        }

        sb.AppendLine("        <footer>");
        sb.AppendLine("            <p>Generated by DataTransfer.Console.Tests • CliWrap Integration Tests</p>");
        sb.AppendLine($"            <p>Test run completed in {captures.Sum(c => c.Duration.TotalSeconds):F2} seconds</p>");
        sb.AppendLine("        </footer>");
        sb.AppendLine("    </div>");
        sb.AppendLine("</body>");
        sb.AppendLine("</html>");

        return sb.ToString();
    }

    private string FormatTestName(string testName)
    {
        return System.Text.RegularExpressions.Regex.Replace(testName, "([a-z])([A-Z])", "$1 $2");
    }

    private string FormatStepName(string stepName)
    {
        return string.Join(" ", stepName.Split('_')
            .Select(w => char.ToUpper(w[0]) + w.Substring(1).ToLower()));
    }

    private string EscapeHtml(string text)
    {
        return text
            .Replace("&", "&amp;")
            .Replace("<", "&lt;")
            .Replace(">", "&gt;")
            .Replace("\"", "&quot;")
            .Replace("'", "&#39;");
    }
}
