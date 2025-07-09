import json
import re
from sqlglot import Tokenizer, parse_one, errors
from sqlglot.expressions import Create, Insert, Select, Table

def ast_to_dict(node):
    def safe_serialize(v):
        if hasattr(v, 'args'):
            return ast_to_dict(v)
        if isinstance(v, list):
            return [safe_serialize(i) for i in v]
        if isinstance(v, (str, int, float, bool)) or v is None:
            return v
        return str(v)

    res = {'class': node.__class__.__name__}
    if hasattr(node, 'args') and node.args:
        res['args'] = {k: safe_serialize(val) for k, val in node.args.items()}
    return res

def validate(ast):
    if isinstance(ast, Create):
        if not ast.this:
            raise ValueError("!!! CREATE missing table name")
        columns = ast.this.args.get("expressions") if ast.this else None
        if not columns:
            raise ValueError("!!! CREATE must define columns")
        seen = set()
        for col in columns:
            if col.__class__.__name__ == "ColumnDef":
                col_name = col.this.name
                col_type = col.args.get("kind")
                if not col_type:
                    raise ValueError(f"!!! Column '{col_name}' missing data type")
                if col_name in seen:
                    raise ValueError(f"!!! Duplicate column name '{col_name}'")
                seen.add(col_name)
            elif col.__class__.__name__ == "Identifier":
                col_name = getattr(col, "name", col.args.get("this", "unknown"))
                raise ValueError(f"!!! Column '{col_name}' missing data type")
            else:
                raise ValueError("!!! Unsupported column definition")
    elif isinstance(ast, Insert):
        if not ast.this or not isinstance(ast.this, Table):
            raise ValueError("!!! INSERT missing target table")
        values = ast.args.get("expression")
        if not values or not getattr(values, "expressions", None):
            raise ValueError("!!! INSERT missing VALUES")
    elif isinstance(ast, Select):
        if not ast.expressions:
            raise ValueError("!!! SELECT missing column(s)")
        if not ast.args.get("from"):
            raise ValueError("!!! SELECT missing FROM clause")
    else:
        raise ValueError("!!! Unsupported query type")

def preprocess_sql(sql):
    pattern = r'^\s*CREATE\s+(?!TABLE\b|INDEX\b|VIEW\b|FUNCTION\b)([a-zA-Z_][\w]*)\s*\('
    return re.sub(pattern, r'CREATE TABLE \1 (', sql, flags=re.IGNORECASE)

def work():
    print("Введіть SQL-запит (або 'exit' для виходу):")
    tokenizer = Tokenizer()
    while True:
        sql = input("SQL> ").strip()
        if sql.lower() in ('exit', 'quit'):
            print("Вихід з програми.")
            break
        if not sql:
            continue
        sql = preprocess_sql(sql)
        try:
            tokens = tokenizer.tokenize(sql)
        except Exception as e:
            print(f"!!! Помилка токенізації: {e}")
            continue
        print("\n=== Токени ===")
        for t in tokens:
            print(f"{t.token_type}: {t.text}")
        print("\n=== AST ===")
        try:
            ast = parse_one(sql)
            print(json.dumps(ast_to_dict(ast), indent=2, ensure_ascii=False))
            validate(ast)
            print("\n+++ Валідація пройдена\n")
            if isinstance(ast, Create):
                table_name = ast.this.this.name if ast.this and ast.this.this else "unknown"
                cols = ast.this.args.get("expressions") or []
                col_strs = []
                for c in cols:
                    if c.__class__.__name__ == "ColumnDef":
                        col_name = c.this.name
                        col_type = c.args.get("kind")
                        col_type_str = col_type.sql() if col_type else "UNKNOWN"
                        col_strs.append(f"{col_name} {col_type_str}")
                    else:
                        col_strs.append("UNKNOWN_COLUMN")
                print(f"Створюємо таблицю '{table_name}' з колонками: {', '.join(col_strs)}")
            elif isinstance(ast, Insert):
                table_name = ast.this.name
                values = ast.args.get("expression").expressions
                vals_str = ", ".join(
                    "(" + ", ".join(v.sql() for v in tup.expressions) + ")" for tup in values
                )
                print(f"Вставляємо в '{table_name}' значення: {vals_str}")
            elif isinstance(ast, Select):
                cols = ", ".join(expr.sql() for expr in ast.expressions)
                from_exprs = ast.args.get("from").expressions
                tables = ", ".join(t.this.name for t in from_exprs)
                print(f"Вибираємо {cols} з таблиці(ць): {tables}")
            else:
                print("Виконання для цього типу запиту не реалізовано")
        except errors.ParseError as e:
            print(f"!!! Помилка парсингу: {e}")
        except ValueError as ve:
            print(str(ve))
        except Exception as ex:
            print(f"!!! Неочікувана помилка: {ex}")
        print()


work()
