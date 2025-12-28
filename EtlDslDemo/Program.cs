using Antlr4.Runtime;
using EtlDsl.Executor;
using EtlDsl.Model;
using EtlDsl.Executor; //java -jar "C:\Users\ACER\Downloads\antlr4-4.13.1-complete.jar" -Dlanguage=CSharp -visitor -o Generated Grammar\EtlDsl.g4

// ---------------- 1️⃣ Define your DSL ----------------
string dsl = @"
PIPELINE MultiSourceComplex VERSION 1.0

EXTRACT CSV ""Data/sales.csv"" AS sales,
        CSV ""Data/discount.csv"" AS discount

TRANSFORM {

    sales {
        MAP IF sales.quantity > 10 THEN sales.price * 1.2 ELSE sales.price TO adjusted_price
        MAP IF sales.adjusted_price > 20 THEN ""HighValue"" ELSE ""Normal"" TO price_category
        FILTER sales.quantity > 5
        AGGREGATE SUM(sales.quantity) AS total_quantity GROUPBY sales.product
        AGGREGATE AVG(sales.adjusted_price) AS avg_adjusted_price GROUPBY sales.product
    }


    discount {
        MAP IF discount.discountRate >= 0.1 THEN ""BigDiscount"" ELSE ""SmallDiscount"" TO discount_category
        MAP IF discount.discountRate > 0 THEN discount.discountRate * 100 ELSE 0 TO discount_percentage
        AGGREGATE SUM(discount.discountRate) AS total_discount
    }
}

LOAD SQL ""FactMultiSourceComplex""


";





// 2️⃣ Parse the DSL
var input = new AntlrInputStream(dsl);
var lexer = new EtlDslLexer(input);
var tokens = new CommonTokenStream(lexer);
var parser = new EtlDslParser(tokens);
var tree = parser.pipeline();

// 3️⃣ Build the pipeline
var visitor = new EtlDslVisitorImpl();
Pipeline pipeline = visitor.Build(tree);

// 4️⃣ Validate the pipeline
try
{
    PipelineValidator.Validate(pipeline);
    Console.WriteLine("Pipeline validation passed ✅");
}
catch (Exception ex)
{
    Console.WriteLine($"Pipeline validation failed ❌: {ex.Message}");
}

// 5️⃣ (Optional) Run Fake ETL if validation passed
FakeEtlExecutor.Run(pipeline);