const parser = require('libpg-query');
const util   = require('util');
const fs     = require('fs');
const path   = require('path');

// ディレクトリ設定
const sqlDir   = path.resolve(__dirname, 'plpgsql');
const macroDir = path.resolve(__dirname, 'macros');
const treeDir  = path.resolve(__dirname, 'tree');

// 唯一ID生成用カウンタ
let stmtCounter = 0;
function nextStmtId(prefix) {
  stmtCounter += 1;
  return `${prefix}_${stmtCounter}`;
}

// 再帰的にステートメントを処理し、Jinjaコードを生成
function processStatements(stmts, lines, indent = 2) {
  const pad = ' '.repeat(indent);
  stmts.forEach(stmt => {
    const key = Object.keys(stmt)[0];
    const node = stmt[key];

    if (key.startsWith('PLpgSQL_stmt_raise')) {
      const rawMsg = node.message ?? '';
      const msg    = rawMsg.replace(/"/g, '\\"');
      const lvl    = node.errlevel ?? 0;
      if (lvl >= 16) {
        lines.push(`${pad}{{ exceptions.raise_compiler_error("${msg}") }}`);
      } else if (lvl >= 10) {
        lines.push(`${pad}{{ exceptions.warn("${msg}") }}`);
      } else {
        lines.push(`${pad}{{ log("${msg}", info=true) }}`);
      }
      return;
    }

    switch (key) {
      // FOR .. IN SELECT ステートメントの実装
      case 'PLpgSQL_stmt_fors': {
        const sql = node.query.PLpgSQL_expr.query.trim();
        lines.push(`${pad}{% set records = run_query("${sql.replace(/"/g, '\\"')}") %}`);
        lines.push(`${pad}{% if execute %}`);
        lines.push(`${pad}  {% for rec in records.rows() %}`);
        // ボディ内の assign や return_next を再帰処理
        processStatements(node.body, lines, indent + 4);
        lines.push(`${pad}  {% endfor %}`);
        lines.push(`${pad}{% endif %}`);
        break;
      }
      // RETURN ステートメントの実装（引数なし/あり両対応）
      case 'PLpgSQL_stmt_return': {
        if (node.expr && node.expr.PLpgSQL_expr && node.expr.PLpgSQL_expr.query) {
          const expr = node.expr.PLpgSQL_expr.query.trim();
          lines.push(`${pad}{% do return(${expr}) %}`);
        } else {
          lines.push(`${pad}{% do return() %}`);
        }
        break;
      }
      // RETURN NEXT ステートメントの実装
      case 'PLpgSQL_stmt_return_next': {
        lines.push(`${pad}{% do return_next() %}`);
        break;
      }
      // BEGIN…END ブロックを再帰展開
      case 'PLpgSQL_stmt_block': {
        processStatements(node.body, lines, indent);
        break;
      }
      case 'PLpgSQL_stmt_execsql': {
        if (node.into) {
          const sql = node.sqlstmt.PLpgSQL_expr.query.trim();
          lines.push(`${pad}{% set results = run_query("${sql.replace(/"/g, '\\"')}") %}`);
          lines.push(`${pad}{% if execute %}`);
          const targets = node.target.PLpgSQL_row.fields.map(f => f.name);
          targets.forEach((col, idx) => {
            lines.push(`${pad}  {% set ${col} = results.columns[${idx}].values()[0] %}`);
          });
          lines.push(`${pad}{% endif %}`);
        } else {
          const sql = node.sqlstmt.PLpgSQL_expr.query.trim();
          const id = nextStmtId('exec');
          lines.push(`${pad}{% call statement('${id}') %}`);
          lines.push(`${pad}${sql};`);
          lines.push(`${pad}{% endcall %}`);
        }
        break;
      }
      case 'PLpgSQL_stmt_dynexecute': {
        const raw = node.query.PLpgSQL_expr.query.trim();
        if (/TRUNCATE TABLE/.test(raw)) {
          lines.push(`${pad}{{ truncate_table(to_table) }}`);
        } else if (/INSERT INTO/.test(raw)) {
          lines.push(`${pad}{{ insert_table_from_trace(to_table) }}`);
        } else {
          const id = nextStmtId('dyn');
          lines.push(`${pad}{% call statement('${id}') %}`);
          lines.push(`${pad}${raw};`);
          lines.push(`${pad}{% endcall %}`);
        }
        break;
      }
      case 'PLpgSQL_stmt_assign': {
        const expr = node.expr.PLpgSQL_expr.query.trim();
        const m = expr.match(/(\w+)\s*:=\s*(.+)/);
        if (m) {
          const varName = m[1];
          const value   = m[2];
          lines.push(`${pad}{% set ${varName} = ${value} %}`);
        }
        break;
      }
      case 'PLpgSQL_stmt_if': {
        const cond = node.cond.PLpgSQL_expr.query;
        lines.push(`${pad}{% if ${cond} %}`);
        processStatements(node.then_body, lines, indent + 2);
        if (node.else_body && node.else_body.length) {
          lines.push(`${pad}{% else %}`);
          processStatements(node.else_body, lines, indent + 2);
        }
        lines.push(`${pad}{% endif %}`);
        break;
      }
      case 'PLpgSQL_stmt_case': {
        const caseVar = node.t_expr.PLpgSQL_expr.query.trim();
        node.case_when_list.forEach((whenItem, idx) => {
          const rawExpr = whenItem.PLpgSQL_case_when.expr.PLpgSQL_expr.query;
          const m = rawExpr.match(/IN\s*\(\s*([^)]+)\s*\)/);
          const vals = m ? m[1] : rawExpr;
          const keyword = idx === 0 ? 'if' : 'elif';
          lines.push(`${pad}{% ${keyword} ${caseVar} in (${vals}) %}`);
          processStatements(whenItem.PLpgSQL_case_when.stmts, lines, indent + 2);
        });
        if (node.have_else) {
          lines.push(`${pad}{% else %}`);
          processStatements(node.else_stmts, lines, indent + 2);
        }
        lines.push(`${pad}{% endif %}`);
        break;
      }
      default: {
        lines.push(`${pad}-- unsupported stmt type: ${key}`);
      }
    }
  });
}

// メイン処理: .sql ファイルをマクロ化し、ASTも保存
[macroDir, treeDir].forEach(dir => {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
});

fs.readdirSync(sqlDir)
  .filter(f => f.endsWith('.sql'))
  .forEach(file => {
    const basename = path.basename(file, '.sql');
    const sqlPath   = path.join(sqlDir, file);
    const sql       = fs.readFileSync(sqlPath, 'utf8');
    const tree      = parser.parsePlPgSQLSync(sql);

    // AST結果を保存
    const treeOutput = util.inspect(tree, { showHidden: true, depth: null, colors: false });
    fs.writeFileSync(path.join(treeDir, `${basename}.tree`), treeOutput, 'utf8');
    console.log(`Saved AST tree: ${basename}.tree`);

    // マクロ生成
    const func  = tree[0].PLpgSQL_function;
    const block = func.action.PLpgSQL_stmt_block;
    const lines = [];
    lines.push(`{% macro ${basename}() %}`);

    // DECLARE 節の変数初期化
    func.datums.forEach(d => {
      if (d.PLpgSQL_var) {
        const name = d.PLpgSQL_var.refname.trim();
        const def  = d.PLpgSQL_var.default_val
          ? d.PLpgSQL_var.default_val.PLpgSQL_expr.query.trim()
          : "''";
        lines.push(`  {% set ${name} = ${def} %}`);
      }
    });
    lines.push('');

    // 本体処理
    processStatements(block.body, lines, 2);
    lines.push(`{% endmacro %}`);

    // マクロファイル書き出し
    const outPath = path.join(macroDir, `${basename}.sql`);
    fs.writeFileSync(outPath, lines.join("\n"), 'utf8');
    console.log(`Generated dbt macro: ${basename}.sql`);
  });
