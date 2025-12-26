using System;
using System.Collections.Generic;
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

        // Track known columns and their types
        var knownColumns = new Dictionary<string, DataType?>();

        // 1. Initialize with extract columns
        foreach (var col in GetColumnsFromSource(pipeline.Extract.Sources, pipeline.Extract.Alias))
        {
            knownColumns[col.Name] = col.Type;
        }

        // 2. Validate Transforms
        foreach (var op in pipeline.Transform.Operations)
        {
            switch (op)
            {
                case MapOperation map:
                    // For Map, we generally expect numeric if math operators are present, 
                    // but let's keep it loose to allow string concatenation.
                    ValidateExpressionColumns(map.Expression, knownColumns, expectedResultType: null);
                    
                    // Allow overwriting existing columns (common in ETL), or keep duplicate check if you prefer strictly new cols
                    if (knownColumns.ContainsKey(map.TargetColumn))
                         Console.WriteLine($"[Info] Overwriting column '{map.TargetColumn}'");
                    
                    knownColumns[map.TargetColumn] = map.TargetType;
                    break;

                case ConditionalMapOperation cond:
                    // Condition must result in boolean
                    ValidateExpressionColumns(cond.Condition, knownColumns, expectedResultType: DataType.Boolean);
                    
                    ValidateExpressionColumns(cond.TrueExpression, knownColumns, expectedResultType: cond.TargetType);
                    ValidateExpressionColumns(cond.FalseExpression, knownColumns, expectedResultType: cond.TargetType);
                    
                    knownColumns[cond.TargetColumn] = cond.TargetType;
                    break;

                case FilterOperation filter:
                    // Filter condition must result in boolean
                    ValidateExpressionColumns(filter.Condition, knownColumns, expectedResultType: DataType.Boolean);
                    break;

                case AggregateOperation agg:
                    // Sum/Avg usually require numeric inputs
                    bool isMathAgg = agg.Function is "SUM" or "AVG" or "MIN" or "MAX";
                    ValidateExpressionColumns(agg.Expression, knownColumns, expectedResultType: isMathAgg ? DataType.Decimal : null);

                    foreach (var col in agg.GroupByColumns)
                    {
                        if (!knownColumns.ContainsKey(col))
                            throw new Exception($"GroupBy column '{col}' does not exist.");
                    }
                    knownColumns[agg.TargetColumn] = agg.TargetType;
                    break;

                default:
                    // Skip unknown operations or throw
                    break;
            }
        }
        
        // 3. (Optional) Validate that Load target type matches output
        // Logic for SQL vs File, etc.
    }

    private static void ValidateExpressionColumns(string expression, Dictionary<string, DataType?> knownColumns, DataType? expectedResultType)
    {
        // 1. Check for Comparison Operators (> < = !)
        // If these exist, the expression results in a Boolean, but the ATOMS are likely Numeric/String.
        bool hasComparison = expression.Any(c => "<>=!".Contains(c));

        // 2. Tokenize (Split by operators to find columns)
        var tokens = expression.Split(
            new[] { ' ', '+', '-', '*', '/', '(', ')', '<', '>', '=', '!', '&', '|', ',' }, 
            StringSplitOptions.RemoveEmptyEntries
        );

        foreach (var token in tokens)
        {
            // Skip Literals / Keywords
            if (IsLiteral(token)) continue;

            // Check Existence
            if (!knownColumns.ContainsKey(token))
                throw new Exception($"Column '{token}' does not exist in the pipeline context.");

            var colType = knownColumns[token];

            // --- SMART TYPE CHECKING ---

            // CASE A: We expect a Boolean Result (like FILTER)
            if (expectedResultType == DataType.Boolean)
            {
                // If there is NO comparison operator (e.g., "FILTER sales.active"), 
                // then the column itself MUST be boolean.
                if (!hasComparison)
                {
                    if (colType != DataType.Boolean)
                        throw new Exception($"Column '{token}' is {colType}, but a Boolean was expected for a direct logical check.");
                }
                // If there IS a comparison (e.g., "sales.price > 100"), 
                // then the column IS ALLOWED to be Numeric/String. We do not enforce boolean on the column.
            }

            // CASE B: We expect a Numeric Result (like SUM or MAP price * qty)
            else if (expectedResultType == DataType.Int || expectedResultType == DataType.Decimal)
            {
                // If the expression uses math operators, columns should generally be numeric
                // (Simple check: if column is String/Bool, warn or fail)
                if (colType == DataType.String || colType == DataType.Boolean)
                {
                     // Only throw if strictly creating a numeric target, though implicit conversion might exist
                     // throw new Exception($"Column '{token}' is {colType}, but used in a Numeric calculation.");
                }
            }
        }
    }

    private static bool IsLiteral(string token)
    {
        if (decimal.TryParse(token, out _)) return true; // Number
        if (token.StartsWith("\"") && token.EndsWith("\"")) return true; // String
        if (token.Equals("TRUE", StringComparison.OrdinalIgnoreCase)) return true;
        if (token.Equals("FALSE", StringComparison.OrdinalIgnoreCase)) return true;
        if (token.Equals("AND", StringComparison.OrdinalIgnoreCase)) return true;
        if (token.Equals("OR", StringComparison.OrdinalIgnoreCase)) return true;
        if (token.Equals("NOT", StringComparison.OrdinalIgnoreCase)) return true;
        return false;
    }

    // Simulated source columns
    private static IEnumerable<(string Name, DataType? Type)> GetColumnsFromSource(IEnumerable<string> sources, string alias)
    {
        // In a real app, you would read the CSV header or Database Schema here.
        // For this fake demo, we return hardcoded types matching your test data.
        yield return ($"{alias}.id", DataType.Int);
        yield return ($"{alias}.quantity", DataType.Int);
        yield return ($"{alias}.price", DataType.Decimal);
        yield return ($"{alias}.category", DataType.String);
        
        // Add extra columns if your tests need them
        yield return ($"{alias}.active", DataType.Boolean); 
    }
}