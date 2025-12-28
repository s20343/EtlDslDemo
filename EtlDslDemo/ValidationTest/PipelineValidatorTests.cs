using System;
using EtlDsl.Executor;
using EtlDsl.Model;

public static class PipelineValidatorTests
{
    public static void RunAll()
    {
        TestColumnExistence();
        TestAliasExistence();
        TestTypeCompatibility();
        TestDuplicateColumns();
        TestGroupByColumns();
    }

    // ---------------- 1️⃣ Column Existence ----------------
    private static void TestColumnExistence()
    {
        string dslPass = @"
PIPELINE ColumnExistPass VERSION 1.0
EXTRACT CSV ""Data/sales.csv"" AS sales
TRANSFORM {
    sales {
        MAP sales.quantity * sales.price TO total
    }
}
LOAD SQL ""Fact""";

        string dslFail = @"
PIPELINE ColumnExistFail VERSION 1.0
EXTRACT CSV ""Data/sales.csv"" AS sales
TRANSFORM {
    sales {
        MAP sales.unknown_field * 2 TO total
    }
}
LOAD SQL ""Fact""";

        RunValidation(dslPass, true, "Column existence (pass)");
        RunValidation(dslFail, false, "Column existence (fail)");
    }

    // ---------------- 2️⃣ Alias Existence ----------------
    private static void TestAliasExistence()
    {
        string dslPass = @"
PIPELINE AliasPass VERSION 1.0
EXTRACT CSV ""Data/discount.csv"" AS discount
TRANSFORM {
    discount {
        MAP discount.discountRate * 100 TO percent
    }
}
LOAD SQL ""Fact""";

        string dslFail = @"
PIPELINE AliasFail VERSION 1.0
EXTRACT CSV ""Data/discount.csv"" AS discoun
TRANSFORM {
    discount {
        MAP discount.discountRate * 100 TO percent
    }
}
LOAD SQL ""Fact""";

        RunValidation(dslPass, true, "Alias existence (pass)");
        RunValidation(dslFail, false, "Alias existence (fail)");
    }

    // ---------------- 3️⃣ Type Compatibility ----------------
    private static void TestTypeCompatibility()
    {
        string dslPass = @"
PIPELINE TypePass VERSION 1.0
EXTRACT CSV ""Data/sales.csv"" AS sales
TRANSFORM {
    sales {
        FILTER sales.quantity > 5
    }
}
LOAD SQL ""Fact""";

        string dslFail = @"
PIPELINE TypeFail VERSION 1.0
EXTRACT CSV ""Data/sales.csv"" AS sales
TRANSFORM {
    sales {
        FILTER sales.product
    }
}
LOAD SQL ""Fact""";

        RunValidation(dslPass, true, "Type compatibility (pass)");
        RunValidation(dslFail, false, "Type compatibility (fail)");
    }

    // ---------------- 4️⃣ Duplicate Target Columns ----------------
    private static void TestDuplicateColumns()
    {
        string dslPass = @"
PIPELINE DupPass VERSION 1.0
EXTRACT CSV ""Data/sales.csv"" AS sales
TRANSFORM {
    sales {
        MAP sales.quantity TO qty
        MAP sales.price TO priceVal
    }
}
LOAD SQL ""Fact""";

        string dslFail = @"
PIPELINE DupFail VERSION 1.0
EXTRACT CSV ""Data/sales.csv"" AS sales
TRANSFORM {
    sales {
        MAP sales.quantity TO qty
        MAP sales.price TO qty
    }
}
LOAD SQL ""Fact""";

        RunValidation(dslPass, true, "Duplicate column (pass)");
        RunValidation(dslFail, false, "Duplicate column (fail)");
    }

    // ---------------- 5️⃣ GroupBy Columns ----------------
    private static void TestGroupByColumns()
    {
        string dslPass = @"
PIPELINE GroupByPass VERSION 1.0
EXTRACT CSV ""Data/sales.csv"" AS sales
TRANSFORM {
    sales {
        AGGREGATE SUM(sales.quantity) AS total GROUPBY product
    }
}
LOAD SQL ""Fact""";

        string dslFail = @"
PIPELINE GroupByFail VERSION 1.0
EXTRACT CSV ""Data/sales.csv"" AS sales
TRANSFORM {
    sales {
        AGGREGATE SUM(sales.quantity) AS total GROUPBY unknown_col
    }
}
LOAD SQL ""Fact""";

        RunValidation(dslPass, true, "GroupBy column (pass)");
        RunValidation(dslFail, false, "GroupBy column (fail)");
    }

    // ---------------- Helper Method ----------------
    private static void RunValidation(string dsl, bool shouldPass, string testName)
    {
        try
        {
            var input = new Antlr4.Runtime.AntlrInputStream(dsl);
            var lexer = new EtlDslLexer(input);
            var tokens = new Antlr4.Runtime.CommonTokenStream(lexer);
            var parser = new EtlDslParser(tokens);
            var tree = parser.pipeline();

            var visitor = new EtlDslVisitorImpl();
            Pipeline pipeline = visitor.Build(tree);

            PipelineValidator.Validate(pipeline);

            if (shouldPass)
                Console.WriteLine($"✅ {testName} passed as expected.");
            else
                Console.WriteLine($"❌ {testName} unexpectedly passed! Should have failed.");
        }
        catch (Exception ex)
        {
            if (shouldPass)
                Console.WriteLine($"❌ {testName} unexpectedly failed: {ex.Message}");
            else
                Console.WriteLine($"✅ {testName} failed as expected: {ex.Message}");
        }
    }
}
// using System;
// using EtlDsl.Executor;
//
// class Program
// {
//     static void Main(string[] args)
//     {
//         Console.WriteLine("Running ETL.NET Semantic Validation Tests...\n");
//
//         // ✅ Run all semantic validation tests
//         PipelineValidatorTests.RunAll();
//
//         Console.WriteLine("\nAll tests completed.");
//     }
// }
