grammar EtlDsl;

// ---------------- Pipeline ----------------
pipeline
    : PIPELINE IDENTIFIER VERSION NUMBER
      extract
      transform
      load
      EOF
    ;

// ---------------- Extract ----------------
extract
    : EXTRACT sourceType extractSource (COMMA extractSource)*
    ;

extractSource
    : STRING AS IDENTIFIER
    ;

sourceType
    : CSV
    | SQL
    | FLATFILE
    | CONNECTOR
    ;

targetIdentifier
    : IDENTIFIER
    ;

// ---------------- Input Streams ----------------
inputstream
    : connector
    | table
    | flatfile
    | expression
    | expressionOp
    | fileFlatfile
    ;

connector
    : CONNECTOR expression AS targetIdentifier
    ;

table
    : TABLE dbOperation
    ;

flatfile
    : regularFlatfile
    | fileFlatfile
    ;

regularFlatfile
    : FLATFILE (NAME expression)?
      COLUMNS typedExprList
      (SEPARATION expression)?
      AS targetIdentifier
    ;

fileFlatfile
    : FLATFILE (LPAREN typeOrNullList RPAREN)? AS targetIdentifier
    ;

typedExprList
    : typedExpr (COMMA typedExpr)*
    ;

typedExpr
    : expression type?
    ;

typeOrNullList
    : typeOrNull (COMMA typeOrNull)*
    ;

typeOrNull
    : type
    | 
    ;

// ---------------- Transform ----------------
transform
    : TRANSFORM LBRACE transformStatementOrBlock* RBRACE
    ;

transformStatementOrBlock
    : transformStatement
    | sourceTransformBlock
    ;

sourceTransformBlock
    : IDENTIFIER LBRACE transformStatement* RBRACE
    ;

transformStatement
    : mapStatement
    | filterStatement
    | aggregateStatement
    | distinctStatement
    | deleteDb
    | lookupObStatement
    | lookupDbStatement
    | selectStatement
    | selectDbStatement
    | correlateStatement
    | synchronizedStatement
    | crossApplyStatement
    ;

mapStatement
    : MAP expression TO IDENTIFIER (AS type)?
    | MAP IF expression THEN expression ELSE expression TO IDENTIFIER (AS type)?
    ;

filterStatement
    : FILTER expression
    ;

aggregateStatement
    : AGGREGATE aggregationFunction LPAREN expression RPAREN
      AS targetIdentifier
      type?
      groupByClause?
    ;

distinctStatement
    : DISTINCT expressionList AS targetIdentifier
    ;

deleteDb
    : DELETE dbOperation WHERE expression
    ;

lookupObStatement
    : LOOKUP lookupStatement
    ;

lookupDbStatement
    : LOOKUPDB lookupStatement
    ;

selectStatement
    : SELECT (OBJECT nameObject=expression)? assignmentList AS targetIdentifier
    ;

selectDbStatement
    : SELECTDB dbOperation
    ;

correlateStatement
    : CORRELATE WITH expression SELECT expressionList AS targetIdentifier
    ;

synchronizedStatement
    : SYNCHRONIZED expression AS targetIdentifier
    ;

crossApplyStatement
    : CROSSAPPLY caOperation
    ;

caOperation
    : flatfile
    | selectDbStatement
    | deleteDb
    | lookupObStatement
    | lookupDbStatement
    | expressionOp
    ;

lookupStatement
    : INTO expression ON assignment SELECT expressionList? cache? AS targetIdentifier
    ;

cache
    : NOCACHE (LPAREN NUMBER RPAREN)?
    | FULLCACHE
    ;

// ---------------- DB / Load Helpers ----------------
dbOperation
    : expression inputTableOp* AS targetIdentifier
    ;

inputTableOp
    : whereStatement
    | includeStatement
    | orderbyStatement
    ;

whereStatement
    : WHERE expression (AND expression)* (AS targetIdentifier)?
    ;

includeStatement
    : INCLUDE expressionList
    ;

orderbyStatement
    : ORDERBY expressionList
    ;

assignmentList
    : assignment (COMMA assignment)*
    ;

assignment
    : IDENTIFIER EQUALS expression
    ;

expressionList
    : expression (COMMA expression)*
    ;

expressionOp
    : expression AS targetIdentifier
    ;

// ---------------- Aggregates ----------------
aggregationFunction
    : SUM | AVG | MIN | MAX
    ;

groupByClause
    : GROUPBY groupByItem (COMMA groupByItem)*
    ;

groupByItem
    : IDENTIFIER
    | qualifiedIdentifier
    ;

// ---------------- Expressions ----------------
expression
    : logicalOrExpr
    ;

logicalOrExpr
    : logicalAndExpr (OR logicalAndExpr)*
    ;

logicalAndExpr
    : logicalNotExpr (AND logicalNotExpr)*
    ;

logicalNotExpr
    : NOT logicalNotExpr
    | comparisonExpr
    ;

comparisonExpr
    : additiveExpr ((EQUALS | NOTEQUALS | SMALLER | SMALLEROREQUAL | GREATER | GREATEROREQUAL | CONTAINS | NOTCONTAINS) additiveExpr)?
    ;

additiveExpr
    : multiplicativeExpr ((PLUS | MINUS) multiplicativeExpr)*
    ;

multiplicativeExpr
    : unaryExpr ((STAR | SLASH) unaryExpr)*
    ;

unaryExpr
    : PLUS unaryExpr
    | MINUS unaryExpr
    | atom
    ;

atom
    : LPAREN expression RPAREN
    | qualifiedIdentifier
    | IDENTIFIER
    | NUMBER
    | STRING
    | NULL
    | NOT NULL expression
    ;

qualifiedIdentifier
    : IDENTIFIER DOT IDENTIFIER
    ;

// ---------------- Load / Output ----------------
load
    : LOAD targetType STRING
    ;

outputstream
    : connector
    | outobject
    | flatfile
    ;

outobject
    : TABLE SEEKON expressionList AS targetIdentifier
    ;

targetType
    : SQL
    ;

// ---------------- Types ----------------
type
    : BOOL
    | BYTE
    | CHAR
    | INT
    | DOUBLE
    | DATE
    | STRING_TYPE
    ;

BOOL        : 'boolean';
BYTE        : 'byte';
CHAR        : 'char';
INT         : 'num';
DOUBLE      : 'double';
DATE        : 'date';
STRING_TYPE : 'string';

// ---------------- Lexer ----------------

// Keywords
PIPELINE     : 'PIPELINE';
VERSION      : 'VERSION';
EXTRACT      : 'EXTRACT';
TRANSFORM    : 'TRANSFORM';
LOAD         : 'LOAD';
MAP          : 'MAP';
FILTER       : 'FILTER';
TO           : 'TO';
AS           : 'AS';
IF           : 'IF';
THEN         : 'THEN';
ELSE         : 'ELSE';
SUM          : 'SUM';
AVG          : 'AVG';
MIN          : 'MIN';
MAX          : 'MAX';
AGGREGATE    : 'AGGREGATE';
GROUPBY      : 'GROUPBY';
DISTINCT     : 'DISTINCT';
DELETE       : 'DELETE';
LOOKUP       : 'LOOKUP';
LOOKUPDB     : 'LOOKUPDB';
INTO         : 'INTO';
NOCACHE      : 'NOCACHE';
FULLCACHE    : 'FULLCACHE';
CROSSAPPLY   : 'CROSS APPLY';
CORRELATE    : 'CORRELATE';
FLATFILE     : 'FLATFILE';
NAME         : 'NAME';
PATH         : 'PATH';
COLUMNS      : 'COLUMNS';
SYNCHRONIZED : 'SYNCHRONIZED';
SEPARATION   : 'SEPARATION';
TABLE        : 'TABLE';
INCLUDE      : 'INCLUDE';
CONNECTOR    : 'CONNECTOR';
OBJECT       : 'OBJECT';
NEW          : 'NEW';
AND          : 'AND';
OR           : 'OR';
NOT          : 'NOT';
NULL         : 'NULL';
SELECT       : 'SELECT';
SELECTDB     : 'SELECTDB';
SAVE         : 'SAVE';
SEEKON       : 'SEEKON';
WHERE        : 'WHERE';
WITH         : 'WITH';
ORDERBY      : 'ORDERBY';
ON : 'ON';


// Comparison operators
EQUALS        : '=';
NOTEQUALS     : '!=';
GREATEROREQUAL: '>=';
GREATER       : '>';
SMALLEROREQUAL: '<=';
SMALLER       : '<';
CONTAINS      : 'CONTAINS';
NOTCONTAINS   : 'NOT CONTAINS';

// Arithmetic operators
PLUS  : '+';
MINUS : '-';
STAR  : '*';
SLASH : '/';

// Punctuation
LPAREN : '(';
RPAREN : ')';
LBRACE : '{';
RBRACE : '}';
DOT    : '.';
COMMA  : ',';

// Data source keywords
CSV       : 'CSV';
SQL       : 'SQL';

// ---------------- Literals ----------------
IDENTIFIER
    : [a-zA-Z_][a-zA-Z_0-9]*;

NUMBER
    : [0-9]+ ('.' [0-9]+)?;

STRING
    : '"' (~["\r\n])* '"';

// ---------------- Whitespace ----------------
WS
    : [ \t\r\n]+ -> skip;
