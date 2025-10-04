using System.Text;

namespace DataTransfer.Web.Tests;

/// <summary>
/// Generates an HTML report from captured test screenshots
/// </summary>
public class ScreenshotReportGenerator
{
    private readonly string _screenshotDirectory;
    private readonly string _outputPath;

    public ScreenshotReportGenerator(string screenshotDirectory = "test-results/screenshots", string outputPath = "test-results/TestReport.html")
    {
        _screenshotDirectory = screenshotDirectory;
        _outputPath = outputPath;
    }

    public async Task GenerateReportAsync()
    {
        if (!Directory.Exists(_screenshotDirectory))
        {
            Console.WriteLine($"Screenshot directory not found: {_screenshotDirectory}");
            return;
        }

        var screenshots = Directory.GetFiles(_screenshotDirectory, "*.png")
            .OrderBy(f => Path.GetFileName(f))
            .ToList();

        if (!screenshots.Any())
        {
            Console.WriteLine("No screenshots found to generate report");
            return;
        }

        var html = GenerateHtml(screenshots);
        var outputDir = Path.GetDirectoryName(_outputPath);
        if (!string.IsNullOrEmpty(outputDir))
        {
            Directory.CreateDirectory(outputDir);
        }

        await File.WriteAllTextAsync(_outputPath, html);
        Console.WriteLine($"Screenshot report generated: {Path.GetFullPath(_outputPath)}");
        Console.WriteLine($"Total screenshots: {screenshots.Count}");
    }

    private string GenerateHtml(List<string> screenshots)
    {
        var sb = new StringBuilder();

        sb.AppendLine("<!DOCTYPE html>");
        sb.AppendLine("<html lang='en'>");
        sb.AppendLine("<head>");
        sb.AppendLine("    <meta charset='UTF-8'>");
        sb.AppendLine("    <meta name='viewport' content='width=device-width, initial-scale=1.0'>");
        sb.AppendLine("    <title>DataTransfer Web UI - Test Screenshots</title>");
        sb.AppendLine("    <style>");
        sb.AppendLine("        * { margin: 0; padding: 0; box-sizing: border-box; }");
        sb.AppendLine("        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif; background: #f5f5f5; padding: 20px; }");
        sb.AppendLine("        .container { max-width: 1400px; margin: 0 auto; background: white; padding: 40px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }");
        sb.AppendLine("        h1 { color: #333; margin-bottom: 10px; font-size: 2.5em; }");
        sb.AppendLine("        .subtitle { color: #666; margin-bottom: 30px; font-size: 1.1em; }");
        sb.AppendLine("        .meta { background: #f8f9fa; padding: 15px; border-radius: 8px; margin-bottom: 40px; }");
        sb.AppendLine("        .meta p { margin: 5px 0; color: #555; }");
        sb.AppendLine("        .test-section { margin-bottom: 60px; }");
        sb.AppendLine("        .test-section h2 { color: #2c3e50; margin-bottom: 20px; padding-bottom: 10px; border-bottom: 3px solid #3498db; }");
        sb.AppendLine("        .screenshot-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(400px, 1fr)); gap: 30px; }");
        sb.AppendLine("        .screenshot-item { background: #fff; border: 1px solid #ddd; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 4px rgba(0,0,0,0.1); transition: transform 0.2s; }");
        sb.AppendLine("        .screenshot-item:hover { transform: translateY(-5px); box-shadow: 0 4px 12px rgba(0,0,0,0.15); }");
        sb.AppendLine("        .screenshot-item img { width: 100%; height: auto; display: block; cursor: pointer; }");
        sb.AppendLine("        .screenshot-caption { padding: 15px; background: #f8f9fa; }");
        sb.AppendLine("        .screenshot-caption h3 { font-size: 0.95em; color: #2c3e50; margin-bottom: 5px; }");
        sb.AppendLine("        .screenshot-caption p { font-size: 0.85em; color: #7f8c8d; }");
        sb.AppendLine("        .modal { display: none; position: fixed; z-index: 1000; left: 0; top: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.9); }");
        sb.AppendLine("        .modal-content { margin: auto; display: block; max-width: 90%; max-height: 90%; margin-top: 2%; }");
        sb.AppendLine("        .close { position: absolute; top: 30px; right: 45px; color: #fff; font-size: 40px; font-weight: bold; cursor: pointer; }");
        sb.AppendLine("        .close:hover { color: #bbb; }");
        sb.AppendLine("        footer { margin-top: 60px; text-align: center; color: #999; font-size: 0.9em; padding-top: 20px; border-top: 1px solid #eee; }");
        sb.AppendLine("    </style>");
        sb.AppendLine("</head>");
        sb.AppendLine("<body>");
        sb.AppendLine("    <div class='container'>");
        sb.AppendLine("        <h1>🚀 DataTransfer Web UI</h1>");
        sb.AppendLine("        <p class='subtitle'>Automated Test Screenshot Report</p>");
        sb.AppendLine("        <div class='meta'>");
        sb.AppendLine($"            <p><strong>Generated:</strong> {DateTime.Now:yyyy-MM-dd HH:mm:ss}</p>");
        sb.AppendLine($"            <p><strong>Total Screenshots:</strong> {screenshots.Count}</p>");
        sb.AppendLine("            <p><strong>Purpose:</strong> Visual documentation of UI functionality and regression testing</p>");
        sb.AppendLine("        </div>");

        // Group screenshots by test name
        var groups = screenshots.GroupBy(s => ExtractTestName(Path.GetFileName(s)));

        foreach (var group in groups)
        {
            sb.AppendLine($"        <div class='test-section'>");
            sb.AppendLine($"            <h2>{FormatTestName(group.Key)}</h2>");
            sb.AppendLine($"            <div class='screenshot-grid'>");

            foreach (var screenshot in group)
            {
                var fileName = Path.GetFileName(screenshot);
                var relativePath = Path.Combine("screenshots", fileName);
                var stepName = ExtractStepName(fileName);

                sb.AppendLine($"                <div class='screenshot-item'>");
                sb.AppendLine($"                    <img src='{relativePath}' alt='{stepName}' onclick='openModal(this.src)'>");
                sb.AppendLine($"                    <div class='screenshot-caption'>");
                sb.AppendLine($"                        <h3>{FormatStepName(stepName)}</h3>");
                sb.AppendLine($"                        <p>{fileName}</p>");
                sb.AppendLine($"                    </div>");
                sb.AppendLine($"                </div>");
            }

            sb.AppendLine($"            </div>");
            sb.AppendLine($"        </div>");
        }

        sb.AppendLine("        <footer>");
        sb.AppendLine("            <p>Generated by DataTransfer.Web.Tests • Playwright E2E Tests</p>");
        sb.AppendLine("            <p>Click any screenshot to view full size</p>");
        sb.AppendLine("        </footer>");
        sb.AppendLine("    </div>");

        // Modal for full-size viewing
        sb.AppendLine("    <div id='modal' class='modal' onclick='closeModal()'>");
        sb.AppendLine("        <span class='close' onclick='closeModal()'>&times;</span>");
        sb.AppendLine("        <img class='modal-content' id='modal-img'>");
        sb.AppendLine("    </div>");

        sb.AppendLine("    <script>");
        sb.AppendLine("        function openModal(src) { document.getElementById('modal').style.display = 'block'; document.getElementById('modal-img').src = src; }");
        sb.AppendLine("        function closeModal() { document.getElementById('modal').style.display = 'none'; }");
        sb.AppendLine("    </script>");
        sb.AppendLine("</body>");
        sb.AppendLine("</html>");

        return sb.ToString();
    }

    private string ExtractTestName(string fileName)
    {
        var parts = fileName.Replace(".png", "").Split('_');
        return parts.Length > 0 ? parts[0] : "Unknown";
    }

    private string ExtractStepName(string fileName)
    {
        var parts = fileName.Replace(".png", "").Split('_');
        return parts.Length > 1 ? string.Join("_", parts.Skip(1)) : fileName;
    }

    private string FormatTestName(string testName)
    {
        // Convert CascadingDropdowns -> Cascading Dropdowns
        return System.Text.RegularExpressions.Regex.Replace(testName, "([a-z])([A-Z])", "$1 $2");
    }

    private string FormatStepName(string stepName)
    {
        // Convert snake_case to Title Case
        return string.Join(" ", stepName.Split('_')
            .Select(w => char.ToUpper(w[0]) + w.Substring(1)));
    }
}
