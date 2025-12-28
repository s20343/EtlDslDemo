using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using EtlDsl.Model;

public static class PipelineValidator
{
    public static void Validate(Pipeline pipeline)
    {
        if (pipeline.Extract == null)
            throw new Exception("Pipeline must have an Extract step.");

        if (pipeline.Transform == null)
            throw new Exception("Pipeline must have a Transform step.");

        if (pipeline.Load == null)
            throw new Exception("Pipeline must have a Load step.");

        // Track known columns globally
        var knownColumns = new Dictionary<string, DataType?>(StringComparer.OrdinalIgnoreCase);

        // 1️⃣ Initialize with headers from CSV files (alias-qualified)
        foreach (var src in pipeline.Extract.SourcesWithAlias)
        {
            foreach (var col in GetColumnsFromCsv(src.Path, src.Alias))
            {
                knownColumns[col.Name] = col.Type;
            }
        }

        // 2️⃣ Validate Source Blocks
        if (pipeline.Transform.SourceBlocks != null)
        {
            foreach (var block in pipeline.Transform.SourceBlocks)
            {
                ValidateOperations(block.Operations, knownColumns, block.SourceAlias);
            }
        }

        // 3️⃣ Validate Global Operations (outside any block)
        if (pipeline.Transform.Operations != null)
        {
            ValidateOperations(pipeline.Transform.Operations, knownColumns);
        }
    }

    private static void ValidateOperations(
        List<IOperation> operations,
        Dictionary<string, DataType?> knownColumns,
        string sourceAlias = null)
    {
        var localColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var op in operations)
        {
            string target = op switch
            {
                MapOperation map => map.TargetColumn,
                ConditionalMapOperation cond => cond.TargetColumn,
                AggregateOperation agg => agg.TargetColumn,
                _ => null
            };

            // Check duplicates in local scope
            if (target != null)
            {
                string qualified = sourceAlias != null ? $"{sourceAlias}.{target}" : target;
                if (localColumns.Contains(qualified))
                    throw new Exception($"Duplicate column '{qualified}' in the same scope.");
                localColumns.Add(qualified);
            }

            switch (op)
            {
                case MapOperation map:
                    ValidateExpressionColumns(map.Expression, knownColumns, sourceAlias);
                    knownColumns[sourceAlias != null ? $"{sourceAlias}.{map.TargetColumn}" : map.TargetColumn] = map.TargetType;
                    break;

                case ConditionalMapOperation cond:
                    ValidateExpressionColumns(cond.Condition, knownColumns, sourceAlias, DataType.Boolean);
                    ValidateExpressionColumns(cond.TrueExpression, knownColumns, sourceAlias, cond.TargetType);
                    ValidateExpressionColumns(cond.FalseExpression, knownColumns, sourceAlias, cond.TargetType);
                    knownColumns[sourceAlias != null ? $"{sourceAlias}.{cond.TargetColumn}" : cond.TargetColumn] = cond.TargetType;
                    break;

                case FilterOperation filter:
                    ValidateExpressionColumns(filter.Condition, knownColumns, sourceAlias, DataType.Boolean);
                    break;

                case AggregateOperation agg:
                    bool isNumericAgg = agg.Function is "SUM" or "AVG" or "MIN" or "MAX";
                    ValidateExpressionColumns(agg.Expression, knownColumns, sourceAlias, isNumericAgg ? DataType.Decimal : null);

                    foreach (var col in agg.GroupByColumns)
                    {
                        string qualifiedCol = col.Contains('.') ? col : (sourceAlias != null ? $"{sourceAlias}.{col}" : col);
                        if (!knownColumns.ContainsKey(qualifiedCol))
                            throw new Exception($"GroupBy column '{qualifiedCol}' does not exist. Available: {string.Join(", ", knownColumns.Keys)}");
                    }

                    knownColumns[sourceAlias != null ? $"{sourceAlias}.{agg.TargetColumn}" : agg.TargetColumn] = agg.TargetType;
                    break;
            }
        }
    }

    private static void ValidateExpressionColumns(
        string expression,
        Dictionary<string, DataType?> knownColumns,
        string sourceAlias = null,
        DataType? expectedResultType = null)
    {
        if (string.IsNullOrWhiteSpace(expression)) return;

        var tokens = expression.Split(
            new[] { ' ', '+', '-', '*', '/', '(', ')', '<', '>', '=', '!', '&', '|', ',' },
            StringSplitOptions.RemoveEmptyEntries
        );

        foreach (var token in tokens)
        {
            var cleanToken = token.Trim();
            if (IsLiteral(cleanToken)) continue;

            // Resolve alias
            string qualifiedToken = cleanToken.Contains('.') ? cleanToken : (sourceAlias != null ? $"{sourceAlias}.{cleanToken}" : cleanToken);

            if (!knownColumns.ContainsKey(qualifiedToken))
                throw new Exception($"Validation Failed: Column '{cleanToken}' (resolved as '{qualifiedToken}') does not exist. Available columns: {string.Join(", ", knownColumns.Keys)}");

            var colType = knownColumns[qualifiedToken];
            if (expectedResultType == DataType.Boolean && !HasComparisonOperators(expression))
            {
                if (colType != DataType.Boolean && colType != null)
                    throw new Exception($"Column '{qualifiedToken}' is {colType}, but Boolean expected.");
            }
        }
    }

    private static bool IsLiteral(string token)
    {
        if (string.IsNullOrWhiteSpace(token)) return true;
        if (decimal.TryParse(token, out _)) return true;
        if (token.StartsWith("\"") && token.EndsWith("\"")) return true;

        var keywords = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "TRUE", "FALSE", "AND", "OR", "NOT", "NULL", "CONTAINS"
        };
        return keywords.Contains(token);
    }

    private static bool HasComparisonOperators(string expr)
    {
        return expr.Any(c => "<>=!".Contains(c)) || expr.Contains("CONTAINS");
    }

    private static IEnumerable<(string Name, DataType? Type)> GetColumnsFromCsv(string path, string alias)
    {
        if (!File.Exists(path))
            throw new Exception($"Extract source file not found: {path}");

        using var reader = new StreamReader(path);
        var headerLine = reader.ReadLine();
        if (string.IsNullOrEmpty(headerLine)) yield break;

        var headers = headerLine.Split(',');
        foreach (var header in headers)
        {
            var cleanHeader = header.Trim().Trim('"');
            yield return ($"{alias}.{cleanHeader}", DataType.Decimal); // default type
        }
    }
}
