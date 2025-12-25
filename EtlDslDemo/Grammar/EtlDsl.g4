grammar EtlDsl;

pipeline
    : PIPELINE IDENTIFIER VERSION NUMBER
      extract
      transform
      load
      EOF
    ;

extract
    : EXTRACT sourceType sourceList AS IDENTIFIER
    ;

sourceList
    : STRING (',' STRING)*
    ;


sourceType
    : CSV
    | SQL
    ;

transform
    : TRANSFORM LBRACE transformStatement* RBRACE
    ;

transformStatement
    : mapStatement
    | filterStatement
    | aggregateStatement
    ;

aggregateStatement
    : AGGREGATE aggregationFunction LPAREN expression RPAREN
      AS targetIdentifier
      type?                     // optional type
      groupByClause?            // optional GROUPBY
    ;

targetIdentifier
    : IDENTIFIER
    ;

groupByClause
    : GROUPBY groupByItem (COMMA groupByItem)*
    ;

groupByItem
    : IDENTIFIER
    | qualifiedIdentifier
    ;


aggregationFunction
    : SUM
    | AVG
    | MIN
    | MAX
    ;

mapStatement
    : MAP expression TO IDENTIFIER (AS type)?
    | MAP IF expression THEN expression ELSE expression TO IDENTIFIER (AS type)?
    ;

filterStatement
    : FILTER expression
    ;

load
    : LOAD targetType STRING
    ;

targetType
    : SQL
    ;

expression
    : expression op=(STAR|SLASH) expression      # MulDivExpr
    | expression op=(PLUS|MINUS) expression      # AddSubExpr
    | expression op=(GT|LT|GTE|LTE|EQ) expression # CompareExpr
    | LPAREN expression RPAREN                    # ParensExpr
    | qualifiedIdentifier                         # QualifiedIdExpr
    | IDENTIFIER                                  # IdExpr
    | NUMBER                                      # NumberExpr
    | STRING                                      # StringExpr
    ;



qualifiedIdentifier
    : IDENTIFIER DOT IDENTIFIER
    ;

operator
    : PLUS | MINUS | STAR | SLASH
    | GT | LT | GTE | LTE | EQ
    ;

type
    : INT
    | DECIMAL
    | STRING_TYPE
    ;

/* Lexer rules */

PIPELINE  : 'PIPELINE';
VERSION   : 'VERSION';
EXTRACT   : 'EXTRACT';
TRANSFORM : 'TRANSFORM';
LOAD      : 'LOAD';
MAP       : 'MAP';
FILTER    : 'FILTER';
TO        : 'TO';
AS        : 'AS';
IF    : 'IF';
THEN  : 'THEN';
ELSE  : 'ELSE';
SUM : 'SUM';
AVG : 'AVG';
MIN : 'MIN';
MAX : 'MAX';

AGGREGATE : 'AGGREGATE';
GROUPBY : 'GROUPBY';
COMMA : ',';


CSV       : 'csv';
SQL       : 'sql';

INT       : 'INT';
DECIMAL   : 'DECIMAL';
STRING_TYPE : 'STRING';

LBRACE : '{';
RBRACE : '}';
LPAREN : '(';
RPAREN : ')';
DOT    : '.';

PLUS  : '+';
MINUS : '-';
STAR  : '*';
SLASH : '/';

GT  : '>';
LT  : '<';
GTE : '>=';
LTE : '<=';
EQ  : '=';

IDENTIFIER
    : [a-zA-Z_][a-zA-Z_0-9]*
    ;

NUMBER
    : [0-9]+ ('.' [0-9]+)?
    ;

STRING
    : '"' (~["\r\n])* '"'
    ;

WS
    : [ \t\r\n]+ -> skip
    ;
