using EtlDsl.Model;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;

namespace EtlDsl.Executor;

public static class FakeEtlExecutor
{
    public static void Run(Pipeline pipeline)
    {
        // Step 0: Extract CSVs
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
                        bool condition = Convert.ToBoolean(Evaluate(cond.Condition, context));
                        string expr = condition ? cond.TrueExpression : cond.FalseExpression;
                        var condVal = Evaluate(expr, context);
                        context[cond.TargetColumn] = ConvertType(condVal, cond.TargetType);
                        break;

                    case FilterOperation filterOp:
                        bool keep = Convert.ToBoolean(Evaluate(filterOp.Condition, context));
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
            Console.WriteLine(string.Join(", ", row.Select(kv => $"{kv.Key}={kv.Value}")));
        }
    }

    private static List<Dictionary<string, object>> HandleAggregates(
        List<Dictionary<string, object>> rows,
        List<AggregateOperation> aggregates)
    {
        var finalRows = new List<Dictionary<string, object>>();

        var groupByCols = aggregates.SelectMany(a => a.GroupByColumns).Distinct().ToList();

        if (groupByCols.Any())
        {
            var grouped = rows.GroupBy(
                r => string.Join("|", groupByCols.Select(c => r.ContainsKey(c) ? r[c]?.ToString() ?? "" : "")));

            foreach (var g in grouped)
            {
                var newRow = new Dictionary<string, object>();

                // Copy group-by columns
                foreach (var col in groupByCols)
                    newRow[col] = g.First().ContainsKey(col) ? g.First()[col] : null;

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
                        "COUNT" => values.Count,
                        _ => throw new Exception($"Unknown aggregation {agg.Function}")
                    };

                    newRow[agg.TargetColumn] = ConvertType(result, agg.TargetType);
                }

                finalRows.Add(newRow);
            }
        }
        else
        {
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
                    "COUNT" => values.Count,
                    _ => throw new Exception($"Unknown aggregation {agg.Function}")
                };

                newRow[agg.TargetColumn] = ConvertType(result, agg.TargetType);
            }

            finalRows.Add(newRow);
        }

        return finalRows;
    }

    private static List<Dictionary<string, object>> ReadCsv(ExtractStep extract)
    {
        var allRows = new List<Dictionary<string, object>>();
        var allColumns = new HashSet<string>();

        var fileRows = new List<List<Dictionary<string, object>>>();

        foreach (var path in extract.Sources)
        {
            if (!File.Exists(path)) continue;

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

        // Merge all files
        foreach (var rows in fileRows)
        {
            foreach (var row in rows)
            {
                foreach (var col in allColumns)
                    if (!row.ContainsKey(col))
                        row[col] = null;

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
    expr = expr.Trim();

    // Parentheses
    while (expr.Contains("("))
    {
        int start = expr.LastIndexOf('(');
        int end = expr.IndexOf(')', start);
        var inner = expr.Substring(start + 1, end - start - 1);
        var val = Evaluate(inner, ctx);
        expr = expr.Substring(0, start) + val + expr.Substring(end + 1);
    }

    // Logical operators: AND / OR
    int andIndex = IndexOfTopLevelOperator(expr, "AND");
    if (andIndex >= 0)
    {
        var left = expr.Substring(0, andIndex);
        var right = expr.Substring(andIndex + 3);
        return Convert.ToBoolean(Evaluate(left, ctx)) && Convert.ToBoolean(Evaluate(right, ctx));
    }

    int orIndex = IndexOfTopLevelOperator(expr, "OR");
    if (orIndex >= 0)
    {
        var left = expr.Substring(0, orIndex);
        var right = expr.Substring(orIndex + 2);
        return Convert.ToBoolean(Evaluate(left, ctx)) || Convert.ToBoolean(Evaluate(right, ctx));
    }

    // Comparison operators
    string[] ops = { "==", "!=", ">=", "<=", ">", "<" };
    foreach (var op in ops)
    {
        int idx = expr.IndexOf(op);
        if (idx >= 0)
        {
            var left = expr.Substring(0, idx).Trim();
            var right = expr.Substring(idx + op.Length).Trim();

            var leftVal = GetValue(left, ctx);
            var rightVal = GetValue(right, ctx);

            bool isNumeric = leftVal is decimal && rightVal is decimal;

            return op switch
            {
                "==" => Equals(leftVal, rightVal),
                "!=" => !Equals(leftVal, rightVal),
                ">" => isNumeric ? (decimal)leftVal > (decimal)rightVal
                                 : throw new Exception("Operator > requires numeric values"),
                "<" => isNumeric ? (decimal)leftVal < (decimal)rightVal
                                 : throw new Exception("Operator < requires numeric values"),
                ">=" => isNumeric ? (decimal)leftVal >= (decimal)rightVal
                                  : throw new Exception("Operator >= requires numeric values"),
                "<=" => isNumeric ? (decimal)leftVal <= (decimal)rightVal
                                  : throw new Exception("Operator <= requires numeric values"),
                _ => throw new Exception($"Unknown operator {op}")
            };
        }
    }

    // Single value: number, quoted string, or column
    return GetValue(expr, ctx);
}

private static int IndexOfTopLevelOperator(string expr, string op)
{
    int level = 0;
    string upper = expr.ToUpperInvariant();
    for (int i = 0; i <= upper.Length - op.Length; i++)
    {
        if (upper[i] == '(') level++;
        else if (upper[i] == ')') level--;
        else if (level == 0 && upper.Substring(i, op.Length) == op)
            return i;
    }
    return -1;
}

private static object GetValue(string token, Dictionary<string, object> ctx)
{
    token = token.Trim();

    // Quoted string literal
    if (token.StartsWith("\"") && token.EndsWith("\""))
        return token.Substring(1, token.Length - 2);

    // Number literal
    if (decimal.TryParse(token, NumberStyles.Any, CultureInfo.InvariantCulture, out var num))
        return num;

    // Column lookup (with alias)
    if (ctx.ContainsKey(token))
        return ctx[token];

    var parts = token.Split('.');
    if (parts.Length == 2 && ctx.ContainsKey(token))
        return ctx[token];
    if (parts.Length == 2 && ctx.ContainsKey(parts[1]))
        return ctx[parts[1]];

    throw new KeyNotFoundException($"Column '{token}' not found in context.");
}




}
