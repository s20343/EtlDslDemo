using EtlDsl.Model;
using System.Data;
using System.Globalization;

namespace EtlDsl.Executor;

public static class FakeEtlExecutor
{
    public static void Run(Pipeline pipeline)
    {
        var rows = ReadCsv(pipeline.Extract);

        Console.WriteLine("\n--- ETL OUTPUT ---");

        // Step 1: Apply MAP / CONDITIONAL MAP / FILTER
        var transformedRows = new List<Dictionary<string, object>>();

        foreach (var row in rows)
        {
            var context = new Dictionary<string, object>(row);
            bool filtered = false;

            foreach (var op in pipeline.Transform.Operations)
            {
                switch (op)
                {
                    case MapOperation map:
                        var mapVal = Evaluate(map.Expression, context);
                        context[map.TargetColumn] = ConvertType(mapVal, map.TargetType);
                        break;

                    case ConditionalMapOperation cond:
                        bool condition = (bool)Evaluate(cond.Condition, context);
                        string expr = condition ? cond.TrueExpression : cond.FalseExpression;
                        var condVal = Evaluate(expr, context);
                        context[cond.TargetColumn] = ConvertType(condVal, cond.TargetType);
                        break;

                    case FilterOperation filter:
                        bool keep = (bool)Evaluate(filter.Condition, context);
                        if (!keep)
                        {
                            filtered = true;
                        }
                        break;
                }
                if (filtered) break;
            }

            if (!filtered)
                transformedRows.Add(context);
        }

        // Step 2: Handle Aggregates
        var aggregates = pipeline.Transform.Operations.OfType<AggregateOperation>().ToList();
        List<Dictionary<string, object>> finalRows = aggregates.Any()
            ? HandleAggregates(transformedRows, aggregates)
            : transformedRows;

        // Step 3: Print final output
        foreach (var row in finalRows)
        {
            Console.WriteLine(string.Join(", ",
                row.Select(kv => $"{kv.Key}={kv.Value}")));
        }
    }

    private static List<Dictionary<string, object>> HandleAggregates(
        List<Dictionary<string, object>> rows,
        List<AggregateOperation> aggregates)
    {
        var finalRows = new List<Dictionary<string, object>>();

        // Collect all group-by columns
        var groupByCols = aggregates.SelectMany(a => a.GroupByColumns).Distinct().ToList();

        if (groupByCols.Any())
        {
            // Group rows by group-by columns (fully qualified)
            var grouped = rows.GroupBy(
                r => string.Join("|", groupByCols.Select(c => r.ContainsKey(c) ? r[c].ToString() : "")));

            foreach (var g in grouped)
            {
                var newRow = new Dictionary<string, object>();

                // Copy group-by columns
                foreach (var col in groupByCols)
                {
                    newRow[col] = g.First().ContainsKey(col) ? g.First()[col] : null;
                }

                // Compute aggregates
                foreach (var agg in aggregates)
                {
                    var values = g
                        .Where(r => r.ContainsKey(agg.Expression))
                        .Select(r => Convert.ToDecimal(Evaluate(agg.Expression, r)))
                        .ToList();

                    decimal result = agg.Function.ToUpper() switch
                    {
                        "SUM" => values.Sum(),
                        "AVG" => values.Any() ? values.Average() : 0,
                        "MIN" => values.Min(),
                        "MAX" => values.Max(),
                        _ => throw new Exception($"Unknown aggregation {agg.Function}")
                    };

                    newRow[agg.TargetColumn] = ConvertType(result, agg.TargetType);
                }

                finalRows.Add(newRow);
            }
        }
        else
        {
            // No GROUPBY: aggregate over all rows
            var newRow = new Dictionary<string, object>();

            foreach (var agg in aggregates)
            {
                var values = rows
                    .Where(r => r.ContainsKey(agg.Expression))
                    .Select(r => Convert.ToDecimal(Evaluate(agg.Expression, r)))
                    .ToList();

                decimal result = agg.Function.ToUpper() switch
                {
                    "SUM" => values.Sum(),
                    "AVG" => values.Any() ? values.Average() : 0,
                    "MIN" => values.Min(),
                    "MAX" => values.Max(),
                    _ => throw new Exception($"Unknown aggregation {agg.Function}")
                };

                newRow[agg.TargetColumn] = ConvertType(result, agg.TargetType);
            }

            finalRows.Add(newRow);
        }

        return finalRows;
    }

    // --- Helpers ---
    private static List<Dictionary<string, object>> ReadCsv(ExtractStep extract)
    {
        var allRows = new List<Dictionary<string, object>>();
        var allColumns = new HashSet<string>();

        // Step 1: Read all CSVs, collect column names
        var fileRows = new List<List<Dictionary<string, object>>>();
        foreach (var path in extract.Sources)
        {
            var lines = File.ReadAllLines(path);
            var headers = lines[0].Split(',');

            allColumns.UnionWith(headers.Select(h => $"{extract.Alias}.{h}"));

            var rows = new List<Dictionary<string, object>>();
            foreach (var line in lines.Skip(1))
            {
                var values = line.Split(',');
                var row = new Dictionary<string, object>();

                for (int i = 0; i < headers.Length; i++)
                {
                    string key = $"{extract.Alias}.{headers[i]}";
                    string val = values[i];

                    if (decimal.TryParse(val, NumberStyles.Any, CultureInfo.InvariantCulture, out var number))
                        row[key] = number;
                    else
                        row[key] = val;
                }

                rows.Add(row);
            }

            fileRows.Add(rows);
        }

        // Step 2: Merge rows from all files
        foreach (var rows in fileRows)
        {
            foreach (var row in rows)
            {
                // Fill missing columns with null
                foreach (var col in allColumns)
                {
                    if (!row.ContainsKey(col))
                        row[col] = null;
                }

                allRows.Add(row);
            }
        }

        return allRows;
    }



    private static object ConvertType(object value, DataType? type)
    {
        if (type == null) return value;

        return type switch
        {
            DataType.Int => Convert.ToInt32(value),
            DataType.Decimal => Convert.ToDecimal(value),
            DataType.String => value.ToString()!,
            _ => value
        };
    }

    private static object Evaluate(string expr, Dictionary<string, object> ctx)
    {
        expr = expr.Replace(" ", "");

        // Handle parentheses recursively
        while (expr.Contains("("))
        {
            int start = expr.LastIndexOf('(');
            int end = expr.IndexOf(')', start);
            var inner = expr.Substring(start + 1, end - start - 1);
            var val = Evaluate(inner, ctx);
            expr = expr.Substring(0, start) + val + expr.Substring(end + 1);
        }

        // Comparison operators
        foreach (var op in new[] { ">=", "<=", ">", "<", "==" })
        {
            int idx = expr.IndexOf(op);
            if (idx > 0)
            {
                var left = expr.Substring(0, idx);
                var right = expr.Substring(idx + op.Length);
                var l = Convert.ToDecimal(Evaluate(left, ctx));
                var r = Convert.ToDecimal(Evaluate(right, ctx));
                return op switch
                {
                    ">" => l > r,
                    "<" => l < r,
                    ">=" => l >= r,
                    "<=" => l <= r,
                    "==" => l == r,
                    _ => throw new Exception()
                };
            }
        }

        // Arithmetic operators
        foreach (var op in new[] { "*", "/", "+", "-" })
        {
            int idx = expr.IndexOf(op);
            if (idx > 0)
            {
                var left = expr.Substring(0, idx);
                var right = expr.Substring(idx + 1);
                var l = Convert.ToDecimal(Evaluate(left, ctx));
                var r = Convert.ToDecimal(Evaluate(right, ctx));
                return op switch
                {
                    "*" => l * r,
                    "/" => l / r,
                    "+" => l + r,
                    "-" => l - r,
                    _ => throw new Exception()
                };
            }
        }

        // Literal
        if (decimal.TryParse(expr, NumberStyles.Any, CultureInfo.InvariantCulture, out var literal))
            return literal;

        // Column with alias
        if (ctx.ContainsKey(expr))
            return Convert.ToDecimal(ctx[expr]);

        // Column without alias (fallback)
        if (expr.Contains("."))
        {
            var col = expr.Split('.')[1];
            if (ctx.ContainsKey(col))
                return Convert.ToDecimal(ctx[col]);
        }

        throw new KeyNotFoundException($"Column '{expr}' not found in context.");
    }
}
