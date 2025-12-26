using EtlDsl.Model;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;

namespace EtlDsl.Executor
{
    public static class FakeEtlExecutor
    {
        public static void Run(Pipeline pipeline)
        {
            Console.WriteLine("\n--- Reading CSV ---");
            var rows = ReadCsv(pipeline.Extract);

            if (!rows.Any())
            {
                Console.WriteLine("No rows found in CSV files.");
                return;
            }

            Console.WriteLine($"Total rows read: {rows.Count}");
            Console.WriteLine("\n--- Applying Transformations ---");

            // Row-level transformations (MAP, ConditionalMap, FILTER)
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
                            context[map.TargetColumn] = ConvertType(Evaluate(map.Expression, context), map.TargetType);
                            break;

                        case ConditionalMapOperation cond:
                            bool condition = Convert.ToBoolean(Evaluate(cond.Condition, context));
                            var expr = condition ? cond.TrueExpression : cond.FalseExpression;
                            context[cond.TargetColumn] = ConvertType(Evaluate(expr, context), cond.TargetType);
                            break;

                        case FilterOperation filter:
                            bool passes = Convert.ToBoolean(Evaluate(filter.Condition, context));
                            if (!passes) filtered = true;
                            break;
                    }
                    if (filtered) break;
                }

                if (!filtered)
                    transformedRows.Add(context);
            }

            // ---------------- Aggregation ----------------
            var aggregates = pipeline.Transform.Operations.OfType<AggregateOperation>().ToList();
            if (aggregates.Any())
            {
                transformedRows = ApplyAggregations(transformedRows, aggregates);
            }

            // ---------------- Output ----------------
            PrintRows(transformedRows);
        }

        private static List<Dictionary<string, object>> ApplyAggregations(
            List<Dictionary<string, object>> rows,
            List<AggregateOperation> aggregates)
        {
            var result = new List<Dictionary<string, object>>();

            foreach (var agg in aggregates)
            {
                // Determine grouping
                var groups = agg.GroupByColumns.Any()
                    ? rows.GroupBy(r => string.Join("|", agg.GroupByColumns.Select(c => r[c])))
                    : new[] { rows.GroupBy(_ => "ALL").First() };

                foreach (var group in groups)
                {
                    var row = new Dictionary<string, object>();

                    // Preserve GROUP BY columns
                    foreach (var col in agg.GroupByColumns)
                        row[col] = group.First()[col];

                    // Evaluate the aggregate expression
                    var values = group.Select(r => Convert.ToDecimal(Evaluate(agg.Expression, r))).ToList();

                    decimal value = agg.Function switch
                    {
                        "SUM" => values.Sum(),
                        "AVG" => values.Average(),
                        "MIN" => values.Min(),
                        "MAX" => values.Max(),
                        _ => throw new Exception("Unknown aggregate function")
                    };

                    row[agg.TargetColumn] = ConvertType(value, agg.TargetType);
                    result.Add(row);
                }
            }

            return result;
        }

        // ---------------- Expression Evaluation ----------------
        public static object Evaluate(string expr, Dictionary<string, object> ctx)
        {
            expr = expr.Trim();
            if (string.IsNullOrEmpty(expr)) return null;

            // Parentheses handling
            while (expr.Contains("("))
            {
                int close = expr.IndexOf(')');
                if (close == -1) break;
                int open = expr.LastIndexOf('(', close);
                if (open == -1) break;

                var inner = expr.Substring(open + 1, close - open - 1);
                var innerValue = Evaluate(inner, ctx);

                string valStr = innerValue switch
                {
                    bool b => b.ToString().ToLower(),
                    decimal d => d.ToString(CultureInfo.InvariantCulture),
                    _ => innerValue?.ToString() ?? "null"
                };

                expr = expr.Substring(0, open) + valStr + expr.Substring(close + 1);
            }

            // Logical operators
            int orIndex = IndexOfTopLevelOperator(expr, " OR ");
            if (orIndex >= 0)
                return Convert.ToBoolean(Evaluate(expr[..orIndex], ctx)) || Convert.ToBoolean(Evaluate(expr[(orIndex + 4)..], ctx));

            int andIndex = IndexOfTopLevelOperator(expr, " AND ");
            if (andIndex >= 0)
                return Convert.ToBoolean(Evaluate(expr[..andIndex], ctx)) && Convert.ToBoolean(Evaluate(expr[(andIndex + 5)..], ctx));

            if (expr.StartsWith("NOT ", StringComparison.OrdinalIgnoreCase))
                return !Convert.ToBoolean(Evaluate(expr.Substring(4), ctx));

            // Comparisons
            string[] cmpOps = { "!=", ">=", "<=", "=", ">", "<" };
            foreach (var op in cmpOps)
            {
                int idx = expr.IndexOf(op, StringComparison.Ordinal);
                if (idx > 0)
                {
                    var left = Evaluate(expr[..idx], ctx);
                    var right = Evaluate(expr[(idx + op.Length)..], ctx);

                    return op switch
                    {
                        "=" => Equals(left, right),
                        "!=" => !Equals(left, right),
                        ">" => Convert.ToDecimal(left) > Convert.ToDecimal(right),
                        "<" => Convert.ToDecimal(left) < Convert.ToDecimal(right),
                        ">=" => Convert.ToDecimal(left) >= Convert.ToDecimal(right),
                        "<=" => Convert.ToDecimal(left) <= Convert.ToDecimal(right),
                        _ => false
                    };
                }
            }

            // Arithmetic operators (respecting precedence)
            int mul = IndexOfTopLevelOperator(expr, "*");
            if (mul >= 0) return Convert.ToDecimal(Evaluate(expr[..mul], ctx)) * Convert.ToDecimal(Evaluate(expr[(mul + 1)..], ctx));
            int div = IndexOfTopLevelOperator(expr, "/");
            if (div >= 0) return Convert.ToDecimal(Evaluate(expr[..div], ctx)) / Convert.ToDecimal(Evaluate(expr[(div + 1)..], ctx));
            int add = IndexOfTopLevelOperator(expr, "+");
            if (add >= 0) return Convert.ToDecimal(Evaluate(expr[..add], ctx)) + Convert.ToDecimal(Evaluate(expr[(add + 1)..], ctx));
            int sub = IndexOfTopLevelOperator(expr, "-");
            if (sub > 0) return Convert.ToDecimal(Evaluate(expr[..sub], ctx)) - Convert.ToDecimal(Evaluate(expr[(sub + 1)..], ctx));

            return GetValue(expr, ctx);
        }

        private static int IndexOfTopLevelOperator(string expr, string op)
        {
            int level = 0;
            var upper = expr.ToUpperInvariant();
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
            if (string.IsNullOrEmpty(token)) return null;
            if (token.Equals("NULL", StringComparison.OrdinalIgnoreCase)) return null;
            if (token.Equals("TRUE", StringComparison.OrdinalIgnoreCase)) return true;
            if (token.Equals("FALSE", StringComparison.OrdinalIgnoreCase)) return false;
            if (token.StartsWith("\"") && token.EndsWith("\"")) return token[1..^1];
            if (decimal.TryParse(token, NumberStyles.Any, CultureInfo.InvariantCulture, out var num)) return num;
            if (ctx.TryGetValue(token, out var value)) return value;
            Console.WriteLine($"[Warn] Column '{token}' not found. Defaulting to 0.");
            return 0;
        }

        private static bool Equals(object l, object r)
        {
            if (l == null && r == null) return true;
            if (l == null || r == null) return false;
            return l.ToString() == r.ToString();
        }

        private static object ConvertType(object value, DataType? type)
        {
            if (type == null) return value;
            return type switch
            {
                DataType.Int => Convert.ToInt32(value),
                DataType.Decimal => Convert.ToDecimal(value),
                DataType.String => value?.ToString(),
                DataType.Boolean => Convert.ToBoolean(value),
                _ => value
            };
        }

        // ---------------- CSV Reading ----------------
        private static List<Dictionary<string, object>> ReadCsv(ExtractStep extract)
        {
            var result = new List<Dictionary<string, object>>();
            foreach (var path in extract.Sources)
            {
                if (!File.Exists(path)) continue;
                var lines = File.ReadAllLines(path);
                if (lines.Length < 2) continue;

                var headers = lines[0].Split(',');
                foreach (var line in lines.Skip(1))
                {
                    if (string.IsNullOrWhiteSpace(line)) continue;
                    var values = line.Split(',');
                    var row = new Dictionary<string, object>();
                    for (int i = 0; i < headers.Length; i++)
                    {
                        var key = $"{extract.Alias}.{headers[i].Trim()}";
                        var val = i < values.Length ? values[i].Trim() : "";
                        if (decimal.TryParse(val, NumberStyles.Any, CultureInfo.InvariantCulture, out var num))
                            row[key] = num;
                        else
                            row[key] = val.Trim('"');
                    }
                    result.Add(row);
                }
            }
            return result;
        }

        private static void PrintRows(List<Dictionary<string, object>> rows)
        {
            Console.WriteLine("\n--- ETL OUTPUT ---");
            if (!rows.Any()) { Console.WriteLine("No rows produced."); return; }
            foreach (var row in rows)
                Console.WriteLine(string.Join(", ", row.Select(kv => $"{kv.Key}={kv.Value}")));
        }
    }
}
