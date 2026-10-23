//! M6 切片 2：S3 Select——SQL 子集（AWS 方言，规格 §12.3 白名单）→ CSV/JSON 求值 → 事件序列。
//!
//! 分层：`parse`（sqlparser AST → 白名单 IR，越界一律 400）→ `evaluate`（对**全量缓冲**的输入
//! 求值——输入缓冲是本片记档取舍；输出侧按 ≤256KiB 分片 + Progress/Stats/End 事件序列化，
//! 线上 event-stream 帧（prelude/headers/CRC32）由 s3s `into_byte_stream` 负责）。
//! Stats 口径（V4 订正）：BytesScanned=输入原始字节（GZIP 取压缩后）、BytesProcessed=送入
//! 求值器的字节（GZIP 为解压后）、BytesReturned=Records 载荷字节；三者独立，**无排序恒等式**
//! （`SELECT 1` 等投影可使 returned > processed；空 GZIP 可 processed < scanned）。
//! Progress 按约 1MiB 输入节奏（启用时；输出/输入任一为 0 时无排序要求）。

use bytes::Bytes;
use serde_json::Value as J;
use sqlparser::ast as A;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// 输出 Records 单帧上限。
const RECORD_CHUNK: usize = 256 * 1024;
/// Progress 事件输入节奏。
const PROGRESS_STEP: i64 = 1024 * 1024;

fn bad(msg: impl Into<String>) -> s3s::S3Error {
    s3s::s3_error!(InvalidRequest, "{}", msg.into())
}

// ---------- CSV/JSON 输入选项 ----------

#[derive(Debug, Clone)]
pub struct CsvIn {
    pub field: u8,
    pub quote: u8,
    pub escape: u8,
    pub record: u8,
    /// AWS 缺省 `#`（Comments 省略时生效）
    pub comments: Option<u8>,
    pub header: HeaderMode,
    /// 引号内是否允许记录分隔符（AWS 缺省 false）
    pub allow_quoted_record_delimiter: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeaderMode {
    None,
    Ignore,
    Use,
}

impl Default for CsvIn {
    fn default() -> Self {
        Self {
            field: b',',
            quote: b'"',
            escape: b'"',
            record: b'\n',
            comments: Some(b'#'),
            header: HeaderMode::None,
            allow_quoted_record_delimiter: false,
        }
    }
}

#[derive(Debug, Clone)]
pub enum InFormat {
    Csv(CsvIn),
    /// true = LINES（逐行 JSON），false = DOCUMENT（整份数组/对象）
    Json {
        lines: bool,
    },
    /// Parquet：行按列名转 JSON 对象（`s.col` 路径；`_N` 位置列不可用）。
    Parquet,
}

#[derive(Debug, Clone)]
pub struct CsvOut {
    pub field: u8,
    pub quote: u8,
    pub escape: u8,
    pub always_quote: bool,
    pub record: u8,
}

impl Default for CsvOut {
    fn default() -> Self {
        Self {
            field: b',',
            quote: b'"',
            escape: b'"',
            always_quote: false,
            record: b'\n',
        }
    }
}

#[derive(Debug, Clone)]
pub enum OutFormat {
    Csv(CsvOut),
    Json { record: u8 },
}

// ---------- SQL 白名单 IR ----------

#[derive(Debug, Clone, PartialEq)]
pub enum ColPath {
    /// CSV 位置列（1 基；header Use 时可由名字解析）
    CsvPos(usize),
    /// JSON 字段路径（`s.a.b`）
    JsonPath(Vec<String>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FnKind {
    Substring,
    Trim,
    Lower,
    Upper,
    CharLength,
    Abs,
    Ceil,
    Floor,
    Round,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggKind {
    CountStar,
    Sum,
    Min,
    Max,
    Avg,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CastTo {
    Int,
    Float,
    Str,
    Bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Path(ColPath),
    Lit(J),
    Bin(BinOp, Box<Expr>, Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        negated: bool,
    },
    Cast(Box<Expr>, CastTo),
    Func(FnKind, Vec<Expr>),
    Agg(AggKind, Option<Box<Expr>>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    All,
    Expr { expr: Expr, name: String },
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectPlan {
    pub items: Vec<SelectItem>,
    pub filter: Option<Expr>,
    pub limit: Option<usize>,
}

/// 解析 SQL → 白名单 IR。子集外语法/函数/列形一律 `InvalidRequest`（不静默错结果）。
pub fn parse(expression: &str) -> s3s::S3Result<SelectPlan> {
    let dialect = GenericDialect {};
    let stmts =
        Parser::parse_sql(&dialect, expression).map_err(|e| bad(format!("SQL 解析失败：{e}")))?;
    if stmts.len() != 1 {
        return Err(bad("只允许单条 SELECT"));
    }
    let A::Statement::Query(q) = &stmts[0] else {
        return Err(bad("只支持 SELECT 语句"));
    };
    // 查询级修饰符：白名单外一律 400（不静默错结果——P1-1）
    if q.with.is_some()
        || q.order_by.is_some()
        || q.fetch.is_some()
        || q.offset.is_some()
        || !q.limit_by.is_empty()
        || !q.locks.is_empty()
        || q.for_clause.is_some()
        || q.settings.is_some()
        || q.format_clause.is_some()
    {
        return Err(bad(
            "不支持 WITH / ORDER BY / OFFSET / FETCH / LIMIT BY / FOR / SETTINGS / FORMAT",
        ));
    }
    let sel = match q.body.as_ref() {
        A::SetExpr::Select(s) => s,
        _ => return Err(bad("只支持简单 SELECT")),
    };
    let group_by_empty = matches!(
        &sel.group_by,
        A::GroupByExpr::Expressions(v, m) if v.is_empty() && m.is_empty()
    );
    if sel.distinct.is_some()
        || !group_by_empty
        || sel.having.is_some()
        || !sel.cluster_by.is_empty()
        || !sel.lateral_views.is_empty()
        || sel.top.is_some()
        || sel.into.is_some()
        || sel.prewhere.is_some()
        || !sel.distribute_by.is_empty()
        || !sel.sort_by.is_empty()
        || sel.qualify.is_some()
        || !sel.named_window.is_empty()
        || sel.value_table_mode.is_some()
        || sel.connect_by.is_some()
    {
        return Err(bad(
            "不支持 DISTINCT / GROUP BY / HAVING / TOP / INTO / WINDOW / QUALIFY 等修饰",
        ));
    }
    // FROM：单表 S3Object [alias]
    if sel.from.len() != 1 {
        return Err(bad("FROM 只允许单表 S3Object"));
    }
    let A::TableWithJoins { relation, joins } = &sel.from[0];
    if !joins.is_empty() {
        return Err(bad("不支持 JOIN"));
    }
    match relation {
        A::TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
        } => {
            if args.is_some()
                || !with_hints.is_empty()
                || version.is_some()
                || *with_ordinality
                || !partitions.is_empty()
                || json_path.is_some()
            {
                return Err(bad("不支持表函数/表提示/版本限定等表修饰"));
            }
            let t = name.to_string();
            if !t.eq_ignore_ascii_case("S3Object") {
                return Err(bad("FROM 只允许 S3Object"));
            }
            if let Some(a) = alias {
                if !a.columns.is_empty() {
                    return Err(bad("不支持别名列定义（s(a,b)）"));
                }
                if !a.name.value.eq_ignore_ascii_case("s") {
                    // 只认别名 s（AWS 缺省 alias 即 s）
                    return Err(bad("S3Object 别名只支持 s"));
                }
            }
        }
        _ => return Err(bad("FROM 只允许 S3Object")),
    }
    // 投影
    let mut items = Vec::new();
    let mut has_agg = false;
    for it in &sel.projection {
        match it {
            A::SelectItem::Wildcard(o) => {
                if o.opt_ilike.is_some()
                    || o.opt_exclude.is_some()
                    || o.opt_except.is_some()
                    || o.opt_replace.is_some()
                    || o.opt_rename.is_some()
                {
                    return Err(bad(
                        "不支持 SELECT * 的 ILIKE/EXCLUDE/EXCEPT/REPLACE/RENAME 修饰",
                    ));
                }
                items.push(SelectItem::All);
            }
            A::SelectItem::UnnamedExpr(e) => {
                let (expr, agg) = conv_expr(e)?;
                has_agg |= agg;
                let name = default_name(&expr);
                items.push(SelectItem::Expr { expr, name });
            }
            A::SelectItem::ExprWithAlias { expr, alias } => {
                let (expr, agg) = conv_expr(expr)?;
                has_agg |= agg;
                items.push(SelectItem::Expr {
                    expr,
                    name: alias.value.clone(),
                });
            }
            _ => return Err(bad("不支持的投影形态")),
        }
    }
    if items.is_empty() {
        return Err(bad("SELECT 列表为空"));
    }
    // SELECT * 不得混排；聚合只允许独占投影顶层（解析期拒绝，与数据内容无关）
    if items.iter().any(|i| matches!(i, SelectItem::All)) && items.len() > 1 {
        return Err(bad("SELECT * 不能与其他表达式混排"));
    }
    if has_agg {
        let sole_top_agg = items.len() == 1
            && matches!(
                &items[0],
                SelectItem::Expr {
                    expr: Expr::Agg(..),
                    ..
                }
            );
        if !sole_top_agg {
            return Err(bad("聚合查询只允许单聚合表达式（无 GROUP BY 支持）"));
        }
    }
    let filter = match &sel.selection {
        Some(e) => {
            let (expr, agg) = conv_expr(e)?;
            if agg {
                return Err(bad("WHERE 中不允许聚合"));
            }
            Some(expr)
        }
        None => None,
    };
    let limit = match &q.limit {
        Some(A::Expr::Value(A::Value::Number(n, _))) => {
            let n: usize = n.parse().map_err(|_| bad("LIMIT 必须是非负整数"))?;
            Some(n)
        }
        Some(_) => return Err(bad("LIMIT 必须是非负整数")),
        None => None,
    };
    if q.offset.is_some() {
        return Err(bad("不支持 OFFSET"));
    }
    Ok(SelectPlan {
        items,
        filter,
        limit,
    })
}

fn default_name(e: &Expr) -> String {
    match e {
        Expr::Path(ColPath::CsvPos(n)) => format!("_{n}"),
        Expr::Path(ColPath::JsonPath(p)) => p.last().cloned().unwrap_or_default(),
        _ => String::new(),
    }
}

/// CEIL/FLOOR 专用 AST 变体（× TO/scale 修饰拒绝）。
fn conv_ceil_floor(
    e: &A::Expr,
    field: &A::CeilFloorKind,
    k: FnKind,
) -> s3s::S3Result<(Expr, bool)> {
    if !matches!(
        field,
        A::CeilFloorKind::DateTimeField(A::DateTimeField::NoDateTime)
    ) {
        return Err(bad("CEIL/FLOOR 不支持 TO/scale 修饰"));
    }
    let (x, agg) = conv_expr(e)?;
    Ok((Expr::Func(k, vec![x]), agg))
}

/// sqlparser AST → IR；返回 (expr, 是否含聚合)。
fn conv_expr(e: &A::Expr) -> s3s::S3Result<(Expr, bool)> {
    let none = |x: Expr| Ok((x, false));
    match e {
        A::Expr::Identifier(id) => Err(bad(format!(
            "列引用必须带 s. 前缀（如 s._1 / s.name）：{}",
            id.value
        ))),
        A::Expr::CompoundIdentifier(parts) => {
            let names: Vec<String> = parts.iter().map(|i| i.value.clone()).collect();
            if names.len() < 2 || !names[0].eq_ignore_ascii_case("s") {
                return Err(bad("列引用必须形如 s._1 / s.a.b"));
            }
            let rest = &names[1..];
            if rest.len() == 1 {
                let col = &rest[0];
                if let Some(n) = col.strip_prefix('_')
                    && let Ok(pos) = n.parse::<usize>()
                {
                    if pos == 0 {
                        return Err(bad("CSV 列为 1 基（s._1 起）"));
                    }
                    return none(Expr::Path(ColPath::CsvPos(pos)));
                }
                return none(Expr::Path(ColPath::JsonPath(vec![col.clone()])));
            }
            none(Expr::Path(ColPath::JsonPath(rest.to_vec())))
        }
        A::Expr::Value(v) => match v {
            A::Value::Number(n, _) => {
                let f: f64 = n.parse().map_err(|_| bad("数值字面量非法"))?;
                let j = if f.fract() == 0.0 && f.abs() < 9e15 {
                    serde_json::json!(f as i64)
                } else {
                    serde_json::json!(f)
                };
                none(Expr::Lit(j))
            }
            A::Value::SingleQuotedString(s) => none(Expr::Lit(J::String(s.clone()))),
            A::Value::Boolean(b) => none(Expr::Lit(J::Bool(*b))),
            A::Value::Null => none(Expr::Lit(J::Null)),
            _ => Err(bad("不支持的字面量形态")),
        },
        A::Expr::Nested(inner) => conv_expr(inner),
        A::Expr::Ceil { expr, field } => conv_ceil_floor(expr, field, FnKind::Ceil),
        A::Expr::Floor { expr, field } => conv_ceil_floor(expr, field, FnKind::Floor),
        A::Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            let (x, mut agg) = conv_expr(expr)?;
            let mut xs = vec![x];
            let Some(frm) = substring_from else {
                return Err(bad("SUBSTRING 需要起点参数"));
            };
            let (f, a1) = conv_expr(frm)?;
            agg |= a1;
            xs.push(f);
            if let Some(for_x) = substring_for {
                let (l, a2) = conv_expr(for_x)?;
                agg |= a2;
                xs.push(l);
            }
            Ok((Expr::Func(FnKind::Substring, xs), agg))
        }
        A::Expr::Trim {
            expr,
            trim_where,
            trim_what,
            trim_characters,
        } => {
            if trim_where.is_some() || trim_what.is_some() || trim_characters.is_some() {
                return Err(bad("TRIM 只支持单参数形态（trim(x)）"));
            }
            let (x, agg) = conv_expr(expr)?;
            Ok((Expr::Func(FnKind::Trim, vec![x]), agg))
        }
        A::Expr::UnaryOp { op, expr } => match op {
            A::UnaryOperator::Not => {
                let (x, agg) = conv_expr(expr)?;
                Ok((Expr::Not(Box::new(x)), agg))
            }
            A::UnaryOperator::Minus => match conv_expr(expr)? {
                (Expr::Lit(J::Number(n)), agg) => {
                    let neg = match n.as_i64() {
                        Some(i) => serde_json::json!(-i),
                        None => serde_json::json!(-n.as_f64().unwrap_or(0.0)),
                    };
                    Ok((Expr::Lit(neg), agg))
                }
                _ => Err(bad("一元负号只支持数值字面量")),
            },
            _ => Err(bad("不支持的一元运算")),
        },
        A::Expr::BinaryOp { left, op, right } => {
            let (l, la) = conv_expr(left)?;
            let (r, ra) = conv_expr(right)?;
            let agg = la || ra;
            let bin = match op {
                A::BinaryOperator::Eq => BinOp::Eq,
                A::BinaryOperator::NotEq => BinOp::Ne,
                A::BinaryOperator::Lt => BinOp::Lt,
                A::BinaryOperator::LtEq => BinOp::Le,
                A::BinaryOperator::Gt => BinOp::Gt,
                A::BinaryOperator::GtEq => BinOp::Ge,
                A::BinaryOperator::And => return Ok((Expr::And(Box::new(l), Box::new(r)), agg)),
                A::BinaryOperator::Or => return Ok((Expr::Or(Box::new(l), Box::new(r)), agg)),
                _ => return Err(bad("不支持的二元运算")),
            };
            Ok((Expr::Bin(bin, Box::new(l), Box::new(r)), agg))
        }
        A::Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let (x, a1) = conv_expr(expr)?;
            let (lo, a2) = conv_expr(low)?;
            let (hi, a3) = conv_expr(high)?;
            Ok((
                Expr::Between {
                    expr: Box::new(x),
                    low: Box::new(lo),
                    high: Box::new(hi),
                    negated: *negated,
                },
                a1 || a2 || a3,
            ))
        }
        A::Expr::InList {
            expr,
            list,
            negated,
        } => {
            let (x, mut agg) = conv_expr(expr)?;
            let mut items = Vec::with_capacity(list.len());
            for i in list {
                let (e, a) = conv_expr(i)?;
                agg |= a;
                items.push(e);
            }
            Ok((
                Expr::InList {
                    expr: Box::new(x),
                    list: items,
                    negated: *negated,
                },
                agg,
            ))
        }
        A::Expr::Like {
            expr,
            pattern,
            negated,
            escape_char,
            any,
        } => {
            if escape_char.is_some() || *any {
                return Err(bad("不支持 LIKE ... ESCAPE / LIKE ANY"));
            }
            let (x, a1) = conv_expr(expr)?;
            let (p, a2) = conv_expr(pattern)?;
            Ok((
                Expr::Like {
                    expr: Box::new(x),
                    pattern: Box::new(p),
                    negated: *negated,
                },
                a1 || a2,
            ))
        }
        A::Expr::IsNull(e) => {
            let (x, a) = conv_expr(e)?;
            Ok((Expr::IsNull(Box::new(x)), a))
        }
        A::Expr::IsNotNull(e) => {
            let (x, a) = conv_expr(e)?;
            Ok((Expr::IsNotNull(Box::new(x)), a))
        }
        A::Expr::Cast {
            kind,
            expr,
            data_type,
            format,
        } => {
            if !matches!(kind, A::CastKind::Cast) || format.is_some() {
                return Err(bad(
                    "只支持 CAST(expr AS type)（TRY_CAST/SAFE_CAST/FORMAT 不支持）",
                ));
            }
            let (x, a) = conv_expr(expr)?;
            let to = match data_type {
                A::DataType::Int(_) | A::DataType::Integer(_) | A::DataType::BigInt(_) => {
                    CastTo::Int
                }
                A::DataType::Float(_) | A::DataType::Real | A::DataType::Double => CastTo::Float,
                A::DataType::Bool | A::DataType::Boolean => CastTo::Bool,
                A::DataType::Varchar(_)
                | A::DataType::Char(_)
                | A::DataType::Text
                | A::DataType::String(_) => CastTo::Str,
                _ => return Err(bad("CAST 目标只支持 INT/FLOAT/STRING/BOOL")),
            };
            Ok((Expr::Cast(Box::new(x), to), a))
        }
        A::Expr::Function(f) => {
            if f.over.is_some()
                || f.filter.is_some()
                || !f.within_group.is_empty()
                || f.null_treatment.is_some()
                || f.uses_odbc_syntax
                || !matches!(f.parameters, A::FunctionArguments::None)
            {
                return Err(bad(
                    "不支持的函数修饰（OVER/FILTER/WITHIN GROUP/NULLS/参数化/ODBC）",
                ));
            }
            let fname = f.name.to_string().to_ascii_lowercase();
            let args = match &f.args {
                A::FunctionArguments::List(l) => {
                    if l.duplicate_treatment.is_some() || !l.clauses.is_empty() {
                        return Err(bad("不支持 DISTINCT/ORDER BY 等函数参数修饰"));
                    }
                    &l.args
                }
                _ => return Err(bad("函数参数形态不支持")),
            };
            // COUNT(*)
            if fname == "count" {
                if args.len() == 1
                    && matches!(
                        &args[0],
                        A::FunctionArg::Unnamed(A::FunctionArgExpr::Wildcard)
                    )
                {
                    return Ok((Expr::Agg(AggKind::CountStar, None), true));
                }
                return Err(bad("只支持 count(*) 聚合"));
            }
            let agg_kind = match fname.as_str() {
                "sum" => Some(AggKind::Sum),
                "min" => Some(AggKind::Min),
                "max" => Some(AggKind::Max),
                "avg" => Some(AggKind::Avg),
                _ => None,
            };
            if let Some(k) = agg_kind {
                if args.len() != 1 {
                    return Err(bad("聚合函数须单参数"));
                }
                let e = match &args[0] {
                    A::FunctionArg::Unnamed(A::FunctionArgExpr::Expr(e)) => e,
                    _ => return Err(bad("聚合参数非法")),
                };
                let (x, a) = conv_expr(e)?;
                if a {
                    return Err(bad("聚合不可嵌套"));
                }
                return Ok((Expr::Agg(k, Some(Box::new(x))), true));
            }
            let kind = match fname.as_str() {
                "substring" | "substr" => FnKind::Substring,
                "trim" => FnKind::Trim,
                "lower" => FnKind::Lower,
                "upper" => FnKind::Upper,
                "char_length" | "character_length" => FnKind::CharLength,
                "abs" => FnKind::Abs,
                "ceil" | "ceiling" => FnKind::Ceil,
                "floor" => FnKind::Floor,
                "round" => FnKind::Round,
                _ => return Err(bad(format!("不支持的函数：{fname}"))),
            };
            // 参数个数在解析期校验（错误码与数据无关）
            let arity_ok = match kind {
                FnKind::Substring => args.len() == 2 || args.len() == 3,
                FnKind::Trim => args.len() == 1 || args.len() == 2,
                FnKind::Round => args.len() == 1 || args.len() == 2,
                _ => args.len() == 1,
            };
            if !arity_ok {
                return Err(bad("函数参数个数非法"));
            }
            let mut xs = Vec::with_capacity(args.len());
            let mut agg = false;
            for a in args {
                let e = match a {
                    A::FunctionArg::Unnamed(A::FunctionArgExpr::Expr(e)) => e,
                    _ => return Err(bad("函数参数非法")),
                };
                let (x, a) = conv_expr(e)?;
                agg |= a;
                xs.push(x);
            }
            Ok((Expr::Func(kind, xs), agg))
        }
        other => Err(bad(format!("不支持的表达式：{other}"))),
    }
}

// ---------- 记录与求值 ----------

/// 一条输入记录（CSV 行 / JSON 对象）。
pub struct Record<'a> {
    pub csv: Option<&'a [String]>,
    pub json: Option<&'a J>,
    /// 记录首字节在输入中的偏移（Progress/Stats 口径用）
    pub offset: i64,
}

#[derive(Debug, Clone, Default)]
pub struct StatsData {
    pub bytes_scanned: i64,
    pub bytes_processed: i64,
    pub bytes_returned: i64,
}

/// 求值后的完整输出（Records 已分片 + 事件序列）。
#[derive(Debug)]
pub enum SelectEvent {
    Records(Bytes),
    Progress(StatsData),
    Stats(StatsData),
    End,
}

pub struct EvalOptions {
    pub request_progress: bool,
    /// R4 口径：BytesScanned = 输入原始字节（GZIP 时压缩后大小）；None = 取送入求值器的字节数。
    pub scanned: Option<i64>,
    /// ScanRange 边界（含端点）：按「记录首字节落在区间内」判定，记录整体处理。
    pub scan: Option<(i64, i64)>,
}

/// 对全量输入求值并组装事件序列（Records ≤256KiB 分片；Progress 1MiB 输入节奏）。
pub fn evaluate(
    plan: &SelectPlan,
    input: &[u8],
    in_fmt: &InFormat,
    out_fmt: &OutFormat,
    opts: &EvalOptions,
) -> s3s::S3Result<Vec<SelectEvent>> {
    evaluate_inner(plan, input, in_fmt, out_fmt, opts, None)
}

/// 控制台一次性 Select 用：加输出上限（记录数 / 渲染字节）。越限返回 `InvalidRequest`
/// （400，不静默截断）——控制台把全部 records 一次读进内存，需要有界失败。
pub fn evaluate_capped(
    plan: &SelectPlan,
    input: &[u8],
    in_fmt: &InFormat,
    out_fmt: &OutFormat,
    opts: &EvalOptions,
    max_records: usize,
    max_bytes: usize,
) -> s3s::S3Result<Vec<SelectEvent>> {
    evaluate_inner(
        plan,
        input,
        in_fmt,
        out_fmt,
        opts,
        Some((max_records, max_bytes)),
    )
}

fn evaluate_inner(
    plan: &SelectPlan,
    input: &[u8],
    in_fmt: &InFormat,
    out_fmt: &OutFormat,
    opts: &EvalOptions,
    caps: Option<(usize, usize)>,
) -> s3s::S3Result<Vec<SelectEvent>> {
    // 1) 解析输入记录（全量缓冲——本片记档取舍）
    enum Parsed {
        Csv(ParsedCsv),
        Json(Vec<(J, i64)>),
    }
    let parsed = match in_fmt {
        InFormat::Csv(c) => Parsed::Csv(parse_csv(input, c)?),
        InFormat::Json { lines } => {
            if opts.scan.is_some() && !*lines {
                return Err(bad("ScanRange 不支持 JSON DOCUMENT 输入"));
            }
            Parsed::Json(parse_json(input, *lines)?)
        }
        InFormat::Parquet => {
            if opts.scan.is_some() {
                return Err(bad("ScanRange 不支持 Parquet 输入"));
            }
            Parsed::Json(parse_parquet(input)?)
        }
    };
    let (csv_header, n_records) = match &parsed {
        Parsed::Csv(p) => (p.header.clone(), p.records.len()),
        Parsed::Json(v) => (None, v.len()),
    };
    let record_at = |i: usize| -> Record<'_> {
        match &parsed {
            Parsed::Csv(p) => {
                let (fields, off) = &p.records[i];
                Record {
                    csv: Some(fields),
                    json: None,
                    offset: *off,
                }
            }
            Parsed::Json(v) => Record {
                csv: None,
                json: Some(&v[i].0),
                offset: v[i].1,
            },
        }
    };
    let processed = input.len() as i64;
    let scanned = opts.scanned.unwrap_or(processed);

    // 2) WHERE 过滤 + 聚合/投影
    let mut out = String::new();
    let mut out_count = 0usize;
    let mut agg_state: Option<AggState> = None;
    let only_agg = matches!(
        plan.items.as_slice(),
        [SelectItem::Expr {
            expr: Expr::Agg(..),
            ..
        }]
    );
    for i in 0..n_records {
        let rec = record_at(i);
        let rec = &rec;
        if let Some((lo, hi)) = opts.scan
            && (rec.offset < lo || rec.offset > hi)
        {
            continue;
        }
        if let Some(f) = &plan.filter
            && bool3(&eval(f, rec, csv_header.as_deref())?)? != Some(true)
        {
            continue;
        }
        if only_agg {
            let st = agg_state.get_or_insert_with(|| AggState::new(&plan.items[0]));
            st.feed(rec, csv_header.as_deref())?;
            continue;
        }
        if plan.limit.is_some_and(|l| out_count >= l) {
            break;
        }
        let line = project(plan, rec, csv_header.as_deref(), out_fmt)?;
        if let Some((max_records, max_bytes)) = caps {
            if out_count >= max_records {
                return Err(bad(format!("结果超过 {max_records} 条记录上限")));
            }
            if out.len() + line.len() > max_bytes {
                return Err(bad(format!("结果超过 {max_bytes} 字节上限")));
            }
        }
        if line.is_empty() {
            // 全表达式为 NULL 且 JSON 输出空对象等——仍输出一条空记录（AWS 输出空行）
        }
        out.push_str(&line);
        out_count += 1;
    }
    if only_agg && plan.limit != Some(0) {
        let st = agg_state.get_or_insert_with(|| AggState::new(&plan.items[0]));
        let line = st.render(out_fmt)?;
        if let Some((_, max_bytes)) = caps
            && line.len() > max_bytes
        {
            return Err(bad(format!("结果超过 {max_bytes} 字节上限")));
        }
        if !line.is_empty() {
            out.push_str(&line);
        }
    }

    // 3) Records 分片 + Progress/Stats/End
    let out_bytes = out.into_bytes();
    let returned = out_bytes.len() as i64;
    let mut events = Vec::new();
    let chunks: Vec<Bytes> = if out_bytes.is_empty() {
        Vec::new()
    } else {
        out_bytes
            .chunks(RECORD_CHUNK)
            .map(Bytes::copy_from_slice)
            .collect()
    };
    // Progress：按 1MiB 输入节奏；均匀散布在 Records 分片之间（确定性规则）
    let progress_n = if opts.request_progress {
        ((scanned / PROGRESS_STEP).max(if scanned > 0 { 1 } else { 0 })) as usize
    } else {
        0
    };
    let n_chunks = chunks.len().max(1);
    let mut next_progress = 0usize;
    for (i, chunk) in chunks.iter().enumerate() {
        // 在第 i 个分片前发出已到期的 Progress（proportional 散布）
        while progress_n > 0
            && next_progress < progress_n
            && (next_progress + 1) * n_chunks / progress_n <= i + 1
        {
            let k = (next_progress + 1) as i64;
            let at = (k * PROGRESS_STEP).min(scanned);
            events.push(SelectEvent::Progress(StatsData {
                bytes_scanned: at,
                bytes_processed: if processed > 0 {
                    (at as i128 * processed as i128 / scanned.max(1) as i128) as i64
                } else {
                    0
                },
                bytes_returned: (returned as i128 * k as i128 / progress_n as i128) as i64,
            }));
            next_progress += 1;
        }
        events.push(SelectEvent::Records(chunk.clone()));
    }
    // 剩余 Progress（无输出时也要有节奏证据）
    while progress_n > 0 && next_progress < progress_n {
        let k = (next_progress + 1) as i64;
        let at = (k * PROGRESS_STEP).min(scanned);
        events.push(SelectEvent::Progress(StatsData {
            bytes_scanned: at,
            bytes_processed: (at as i128 * processed as i128 / scanned.max(1) as i128) as i64,
            bytes_returned: returned,
        }));
        next_progress += 1;
    }
    events.push(SelectEvent::Stats(StatsData {
        bytes_scanned: scanned,
        bytes_processed: processed,
        bytes_returned: returned,
    }));
    events.push(SelectEvent::End);
    Ok(events)
}

struct AggState {
    kind: AggKind,
    expr: Option<Expr>,
    name: String,
    count: i64,
    acc: f64,
    minmax: Option<J>,
}

impl AggState {
    fn new(item: &SelectItem) -> Self {
        let SelectItem::Expr {
            expr: Expr::Agg(k, e),
            name,
        } = item
        else {
            unreachable!("only_agg 已判定")
        };
        Self {
            kind: *k,
            expr: e.as_deref().cloned(),
            // 投影别名即输出列名；无别名按 AWS 约定 _1
            name: if name.is_empty() {
                "_1".to_string()
            } else {
                name.clone()
            },
            count: 0,
            acc: 0.0,
            minmax: None,
        }
    }

    fn feed(&mut self, rec: &Record<'_>, header: Option<&[String]>) -> s3s::S3Result<()> {
        if matches!(self.kind, AggKind::CountStar) {
            self.count += 1;
            return Ok(());
        }
        let v = eval(self.expr.as_ref().unwrap(), rec, header)?;
        if v.is_null() {
            return Ok(());
        }
        match self.kind {
            AggKind::Sum | AggKind::Avg => {
                self.acc += num_of(&v)?;
                self.count += 1;
            }
            AggKind::Min | AggKind::Max => {
                self.minmax = Some(match self.minmax.take() {
                    None => v,
                    Some(cur) => {
                        let ord = cmp_vals(&v, &cur)?;
                        match self.kind {
                            AggKind::Min => {
                                if ord == std::cmp::Ordering::Less {
                                    v
                                } else {
                                    cur
                                }
                            }
                            _ => {
                                if ord == std::cmp::Ordering::Greater {
                                    v
                                } else {
                                    cur
                                }
                            }
                        }
                    }
                });
            }
            AggKind::CountStar => unreachable!(),
        }
        Ok(())
    }

    fn render(&self, out: &OutFormat) -> s3s::S3Result<String> {
        let v = match self.kind {
            AggKind::CountStar => J::from(self.count),
            AggKind::Sum => {
                if self.count == 0 {
                    J::Null
                } else {
                    number_from_f64(self.acc)
                }
            }
            AggKind::Avg => {
                if self.count == 0 {
                    J::Null
                } else {
                    number_from_f64(self.acc / self.count as f64)
                }
            }
            AggKind::Min | AggKind::Max => self.minmax.clone().unwrap_or(J::Null),
        };
        render_row(&[(self.name.clone(), v)], out)
    }
}

fn number_from_f64(f: f64) -> J {
    if f.fract() == 0.0 && f.abs() < 9e15 {
        serde_json::json!(f as i64)
    } else {
        serde_json::json!(f)
    }
}

fn project(
    plan: &SelectPlan,
    rec: &Record<'_>,
    header: Option<&[String]>,
    out: &OutFormat,
) -> s3s::S3Result<String> {
    if matches!(plan.items.as_slice(), [SelectItem::All]) {
        // SELECT *：原记录直出
        return match rec {
            Record { json: Some(j), .. } => match out {
                OutFormat::Json { record } => Ok(format!("{}{}", j, *record as char)),
                OutFormat::Csv(c) => render_csv_values(&[J::String(j.to_string())], c),
            },
            Record {
                csv: Some(fields), ..
            } => {
                let mut kv = Vec::new();
                for (i, f) in fields.iter().enumerate() {
                    let name = header
                        .and_then(|h| h.get(i).cloned())
                        .unwrap_or_else(|| format!("_{}", i + 1));
                    kv.push((name, J::String(f.clone())));
                }
                render_row(&kv, out)
            }
            _ => unreachable!(),
        };
    }
    let mut kv = Vec::new();
    for (pos, it) in plan.items.iter().enumerate() {
        let SelectItem::Expr { expr, name } = it else {
            return Err(bad("SELECT * 不能与列混排"));
        };
        let v = eval(expr, rec, header)?;
        let key = if name.is_empty() {
            let d = default_name(expr);
            if d.is_empty() {
                // 无别名表达式：AWS 约定按列位命名 _N
                format!("_{}", pos + 1)
            } else {
                d
            }
        } else {
            name.clone()
        };
        kv.push((key, v));
    }
    render_row(&kv, out)
}

fn render_row(kv: &[(String, J)], out: &OutFormat) -> s3s::S3Result<String> {
    match out {
        OutFormat::Json { record } => {
            let mut m = serde_json::Map::new();
            for (k, v) in kv {
                m.insert(k.clone(), v.clone());
            }
            Ok(format!("{}{}", J::Object(m), *record as char))
        }
        OutFormat::Csv(c) => {
            render_csv_values(&kv.iter().map(|(_, v)| v.clone()).collect::<Vec<_>>(), c)
        }
    }
}

fn render_csv_values(vals: &[J], c: &CsvOut) -> s3s::S3Result<String> {
    let mut out: Vec<u8> = Vec::new();
    for (i, v) in vals.iter().enumerate() {
        if i > 0 {
            out.push(c.field);
        }
        let raw = match v {
            J::Null => String::new(),
            J::String(x) => x.clone(),
            other => other.to_string(),
        };
        let need_quote = c.always_quote
            || raw
                .bytes()
                .any(|b| b == c.field || b == c.quote || b == c.record || b == b'\n' || b == b'\r');
        if need_quote {
            out.push(c.quote);
            for &b in raw.as_bytes() {
                if c.escape == c.quote {
                    if b == c.quote {
                        out.push(c.quote); // 双写
                    }
                    out.push(b);
                } else {
                    // 自定义转义：引号与转义符自身都需前缀转义（回读对称）
                    if b == c.quote || b == c.escape {
                        out.push(c.escape);
                    }
                    out.push(b);
                }
            }
            out.push(c.quote);
        } else {
            out.extend_from_slice(raw.as_bytes());
        }
    }
    out.push(c.record);
    String::from_utf8(out).map_err(|_| bad("CSV 输出含非 UTF-8 分隔符/引号字节"))
}

// ---------- CSV 解析（自写：注释/记录分隔/引号内换行全可控） ----------

pub struct ParsedCsv {
    pub header: Option<Vec<String>>,
    pub records: Vec<(Vec<String>, i64)>,
    pub bytes_consumed: i64,
}

pub fn parse_csv(input: &[u8], c: &CsvIn) -> s3s::S3Result<ParsedCsv> {
    let mut records: Vec<(Vec<String>, i64)> = Vec::new();
    let mut header: Option<Vec<String>> = None;
    let mut i = 0usize;
    let n = input.len();
    let mut first = true;
    while i < n {
        // 记录起点：注释行整行跳过（不在引号内）
        if let Some(cm) = c.comments
            && input[i] == cm
        {
            while i < n && input[i] != c.record {
                i += 1;
            }
            if i < n {
                i += 1;
            }
            continue;
        }
        let start = i as i64;
        let mut fields: Vec<String> = Vec::new();
        let mut field: Vec<u8> = Vec::new();
        let mut in_quotes = false;
        let mut field_started = false;
        let mut ended_with_delim = false;
        loop {
            if i >= n {
                break;
            }
            let b = input[i];
            if in_quotes {
                if b == c.escape && c.escape != c.quote {
                    // 转义符：下一个字符原样进入
                    i += 1;
                    if i < n {
                        field.push(input[i]);
                    }
                    i += 1;
                    continue;
                }
                if b == c.quote {
                    // 双写引号或结束引号
                    if i + 1 < n && input[i + 1] == c.quote && c.escape == c.quote {
                        field.push(c.quote);
                        i += 2;
                        continue;
                    }
                    in_quotes = false;
                    i += 1;
                    continue;
                }
                if b == c.record && !c.allow_quoted_record_delimiter {
                    return Err(bad(
                        "引号内出现记录分隔符（需 AllowQuotedRecordDelimiter=true）",
                    ));
                }
                field.push(b);
                i += 1;
                continue;
            }
            if b == c.quote && !field_started {
                in_quotes = true;
                field_started = true;
                i += 1;
                continue;
            }
            if b == c.field {
                fields.push(
                    String::from_utf8(std::mem::take(&mut field))
                        .map_err(|_| bad("CSV 字段不是合法 UTF-8"))?,
                );
                field_started = false;
                i += 1;
                continue;
            }
            if b == c.record {
                i += 1;
                fields.push(
                    String::from_utf8(std::mem::take(&mut field))
                        .map_err(|_| bad("CSV 字段不是合法 UTF-8"))?,
                );
                ended_with_delim = true;
                break;
            }
            field.push(b);
            field_started = true;
            i += 1;
        }
        if in_quotes {
            return Err(bad("CSV 引号未闭合"));
        }
        if i == n && !ended_with_delim && fields.is_empty() && field.is_empty() {
            break; // 尾部空行
        }
        if !ended_with_delim {
            fields.push(String::from_utf8(field).map_err(|_| bad("CSV 字段不是合法 UTF-8"))?); // EOF 无记录分隔符：收尾字段
        }
        if first {
            first = false;
            match c.header {
                HeaderMode::Ignore => continue,
                HeaderMode::Use => {
                    header = Some(fields);
                    continue;
                }
                HeaderMode::None => {}
            }
        }
        records.push((fields, start));
    }
    Ok(ParsedCsv {
        header,
        records,
        bytes_consumed: n as i64,
    })
}

// ---------- JSON 解析 ----------

pub fn parse_json(input: &[u8], lines: bool) -> s3s::S3Result<Vec<(J, i64)>> {
    if lines {
        let mut out = Vec::new();
        let mut off = 0usize;
        for line in input.split(|b| *b == b'\n') {
            let line_off = off;
            off += line.len() + 1;
            let t = std::str::from_utf8(line).map_err(|_| bad("JSON 行非 UTF-8"))?;
            let trimmed = t.trim();
            if trimmed.is_empty() {
                continue;
            }
            let lead = t.len() - t.trim_start().len();
            let v =
                serde_json::from_str(trimmed).map_err(|e| bad(format!("JSON 行解析失败：{e}")))?;
            out.push((v, (line_off + lead) as i64));
        }
        Ok(out)
    } else {
        let v: J = serde_json::from_slice(input).map_err(|e| bad(format!("JSON 解析失败：{e}")))?;
        Ok(match v {
            J::Array(a) => a.into_iter().map(|x| (x, 0)).collect(),
            other => vec![(other, 0)],
        })
    }
}

// ---------- Parquet 解析（行 → JSON 对象，复用 JSON 路径求值） ----------

/// 解压后列字节总预算：Parquet 行组是压缩的，解压爆炸需在读取前拒绝
/// （与 GZIP 的 MAX_INFLATED 512MiB 同口径；文件本体仍受 256MiB 输入上限约束）。
pub const MAX_PARQUET_UNCOMPRESSED: i64 = 512 * 1024 * 1024;

/// Parquet 行 → JSON（列名即键）。ScanRange 无行内字节偏移语义，调用方须先拒绝。
pub fn parse_parquet(input: &[u8]) -> s3s::S3Result<Vec<(J, i64)>> {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    let reader = SerializedFileReader::new(Bytes::copy_from_slice(input))
        .map_err(|e| bad(format!("Parquet 解析失败：{e}")))?;
    let uncompressed: i64 = reader
        .metadata()
        .row_groups()
        .iter()
        .map(|rg| rg.total_byte_size())
        .sum();
    if uncompressed > MAX_PARQUET_UNCOMPRESSED {
        return Err(bad("Parquet 解压后超过 512MiB 上限"));
    }
    let iter = reader
        .get_row_iter(None)
        .map_err(|e| bad(format!("Parquet 行读取失败：{e}")))?;
    let mut out = Vec::new();
    for row in iter {
        let row = row.map_err(|e| bad(format!("Parquet 行解码失败：{e}")))?;
        out.push((row.to_json_value(), 0));
    }
    Ok(out)
}

// ---------- 表达式求值（三值逻辑） ----------

/// SQL 三值逻辑：布尔上下文只接受 BOOL/NULL；NULL = Unknown；其他类型 → 400（P2-5）。
fn bool3(v: &J) -> s3s::S3Result<Option<bool>> {
    match v {
        J::Bool(b) => Ok(Some(*b)),
        J::Null => Ok(None),
        _ => Err(bad("布尔表达式需要 BOOLEAN 操作数")),
    }
}

type ExprResult = s3s::S3Result<J>;

pub fn eval(e: &Expr, rec: &Record<'_>, header: Option<&[String]>) -> ExprResult {
    match e {
        Expr::Lit(v) => Ok(v.clone()),
        Expr::Path(p) => Ok(resolve(p, rec, header)),
        Expr::Bin(op, l, r) => {
            let a = eval(l, rec, header)?;
            let b = eval(r, rec, header)?;
            if a.is_null() || b.is_null() {
                return Ok(J::Null);
            }
            let ord = cmp_vals(&a, &b)?;
            use std::cmp::Ordering::*;
            let res = match op {
                BinOp::Eq => ord == Equal,
                BinOp::Ne => ord != Equal,
                BinOp::Lt => ord == Less,
                BinOp::Le => ord != Greater,
                BinOp::Gt => ord == Greater,
                BinOp::Ge => ord != Less,
            };
            Ok(J::Bool(res))
        }
        Expr::And(l, r) => {
            // 真值表：false ∧ anything = false；true ∧ Unknown = Unknown；Unknown ∧ false = false
            let a = bool3(&eval(l, rec, header)?)?;
            if a == Some(false) {
                return Ok(J::Bool(false));
            }
            let b = bool3(&eval(r, rec, header)?)?;
            if b == Some(false) {
                return Ok(J::Bool(false));
            }
            if a == Some(true) && b == Some(true) {
                return Ok(J::Bool(true));
            }
            Ok(J::Null)
        }
        Expr::Or(l, r) => {
            // true ∨ anything = true；false ∨ Unknown = Unknown；Unknown ∨ true = true
            let a = bool3(&eval(l, rec, header)?)?;
            if a == Some(true) {
                return Ok(J::Bool(true));
            }
            let b = bool3(&eval(r, rec, header)?)?;
            if b == Some(true) {
                return Ok(J::Bool(true));
            }
            if a == Some(false) && b == Some(false) {
                return Ok(J::Bool(false));
            }
            Ok(J::Null)
        }
        Expr::Not(x) => match bool3(&eval(x, rec, header)?)? {
            Some(b) => Ok(J::Bool(!b)),
            None => Ok(J::Null),
        },
        Expr::IsNull(x) => Ok(J::Bool(eval(x, rec, header)?.is_null())),
        Expr::IsNotNull(x) => Ok(J::Bool(!eval(x, rec, header)?.is_null())),
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let x = eval(expr, rec, header)?;
            let lo = eval(low, rec, header)?;
            let hi = eval(high, rec, header)?;
            if x.is_null() || lo.is_null() || hi.is_null() {
                return Ok(J::Null);
            }
            let inside = cmp_vals(&x, &lo)? != std::cmp::Ordering::Less
                && cmp_vals(&x, &hi)? != std::cmp::Ordering::Greater;
            Ok(J::Bool(inside != *negated))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let x = eval(expr, rec, header)?;
            if x.is_null() {
                return Ok(J::Null);
            }
            let mut found = false;
            let mut has_null = false;
            for it in list {
                let v = eval(it, rec, header)?;
                if v.is_null() {
                    has_null = true;
                    continue;
                }
                if cmp_vals(&x, &v)? == std::cmp::Ordering::Equal {
                    found = true;
                    break;
                }
            }
            // 未命中且列表含 NULL → Unknown（NOT IN 同）
            match (found, has_null) {
                (true, _) => Ok(J::Bool(!*negated)),
                (false, true) => Ok(J::Null),
                (false, false) => Ok(J::Bool(*negated)),
            }
        }
        Expr::Like {
            expr,
            pattern,
            negated,
        } => {
            let x = eval(expr, rec, header)?;
            let p = eval(pattern, rec, header)?;
            let (J::String(s), J::String(pat)) = (x, p) else {
                return Ok(J::Null);
            };
            if pat.len() > MAX_LIKE_PATTERN {
                return Err(bad("LIKE 模式过长"));
            }
            Ok(J::Bool(like_match(&s, &pat) != *negated))
        }
        Expr::Cast(x, to) => {
            let v = eval(x, rec, header)?;
            cast(v, *to)
        }
        Expr::Func(k, args) => {
            let mut vs = Vec::with_capacity(args.len());
            for a in args {
                vs.push(eval(a, rec, header)?);
            }
            apply_fn(*k, vs)
        }
        Expr::Agg(..) => Err(bad("聚合表达式不可在此处求值")),
    }
}

fn resolve(p: &ColPath, rec: &Record<'_>, header: Option<&[String]>) -> J {
    match p {
        ColPath::CsvPos(n) => match rec.csv {
            Some(fields) => fields
                .get(n - 1)
                .map(|s| J::String(s.clone()))
                .unwrap_or(J::Null),
            None => J::Null,
        },
        ColPath::JsonPath(path) => {
            // CSV + header Use：名字直接查表头
            if let Some(fields) = rec.csv {
                if let Some(h) = header
                    && path.len() == 1
                    && let Some(idx) = h.iter().position(|x| x == &path[0])
                {
                    return fields
                        .get(idx)
                        .map(|s| J::String(s.clone()))
                        .unwrap_or(J::Null);
                }
                return J::Null;
            }
            let mut cur = match rec.json {
                Some(j) => j,
                None => return J::Null,
            };
            for seg in path {
                cur = match cur.get(seg) {
                    Some(v) => v,
                    None => return J::Null,
                };
            }
            cur.clone()
        }
    }
}

fn num_of(v: &J) -> s3s::S3Result<f64> {
    match v {
        J::Number(n) => n.as_f64().ok_or_else(|| bad("数值越界")),
        J::String(s) => s
            .parse::<f64>()
            .map_err(|_| bad(format!("不能转数值：{s}"))),
        J::Bool(b) => Ok(if *b { 1.0 } else { 0.0 }),
        J::Null => Err(bad("NULL 不能转数值")),
        _ => Err(bad("不能转数值（对象/数组）")),
    }
}

fn cmp_vals(a: &J, b: &J) -> s3s::S3Result<std::cmp::Ordering> {
    use std::cmp::Ordering;
    match (a, b) {
        (J::Number(x), J::Number(y)) => {
            let (x, y) = (x.as_f64().unwrap_or(0.0), y.as_f64().unwrap_or(0.0));
            Ok(x.partial_cmp(&y).unwrap_or(Ordering::Equal))
        }
        (J::String(x), J::String(y)) => Ok(x.cmp(y)),
        (J::Bool(x), J::Bool(y)) => Ok(x.cmp(y)),
        // 混合：可数值化则数值比较，否则按文本比较（CSV 列全为字符串，比较需此路径）
        _ => {
            let (x, y) = (scalar_text(a), scalar_text(b));
            if let (Ok(xf), Ok(yf)) = (x.parse::<f64>(), y.parse::<f64>()) {
                return Ok(xf.partial_cmp(&yf).unwrap_or(Ordering::Equal));
            }
            Ok(x.cmp(&y))
        }
    }
}

fn scalar_text(v: &J) -> String {
    match v {
        J::String(s) => s.clone(),
        J::Number(n) => n.to_string(),
        J::Bool(b) => b.to_string(),
        J::Null => String::new(),
        other => other.to_string(),
    }
}

fn cast(v: J, to: CastTo) -> ExprResult {
    if v.is_null() {
        return Ok(J::Null);
    }
    match to {
        CastTo::Int => {
            let f = num_of(&v)?;
            Ok(J::from(f as i64))
        }
        CastTo::Float => Ok(J::from(num_of(&v)?)),
        CastTo::Str => Ok(J::String(match v {
            J::String(s) => s,
            other => other.to_string(),
        })),
        CastTo::Bool => match v {
            J::Bool(b) => Ok(J::Bool(b)),
            J::Number(n) => Ok(J::Bool(n.as_f64().unwrap_or(0.0) != 0.0)),
            J::String(s) => match s.to_ascii_lowercase().as_str() {
                "true" | "1" => Ok(J::Bool(true)),
                "false" | "0" | "" => Ok(J::Bool(false)),
                _ => Err(bad(format!("不能转 BOOL：{s}"))),
            },
            _ => Err(bad("不能转 BOOL")),
        },
    }
}

fn apply_fn(k: FnKind, vs: Vec<J>) -> ExprResult {
    let arg_err = || bad("函数参数个数/类型非法");
    // 参数个数先于 NULL 短路校验（否则零参/多余参数在 NULL 输入下被掩盖）
    let arity_ok = match k {
        FnKind::Substring => vs.len() == 2 || vs.len() == 3,
        FnKind::Trim => vs.len() == 1 || vs.len() == 2,
        FnKind::Round => vs.len() == 1 || vs.len() == 2,
        _ => vs.len() == 1,
    };
    if !arity_ok {
        return Err(arg_err());
    }
    // SQL 标量函数：任一参数 NULL → NULL（含 ceiling(NULL) 不再落成 0）
    if vs.iter().any(|v| v.is_null()) {
        return Ok(J::Null);
    }
    match k {
        FnKind::Substring => {
            if vs.len() != 2 && vs.len() != 3 {
                return Err(arg_err());
            }
            let J::String(s) = &vs[0] else {
                return Ok(J::Null);
            };
            let start = num_of(&vs[1])? as i64;
            let len = if vs.len() == 3 {
                Some(num_of(&vs[2])? as i64)
            } else {
                None
            };
            let chars: Vec<char> = s.chars().collect();
            let begin = (start.max(1) - 1) as usize;
            let end = match len {
                Some(l) if l >= 0 => (begin + l as usize).min(chars.len()),
                Some(_) => return Err(bad("substring 长度不能为负")),
                None => chars.len(),
            };
            if begin >= chars.len() {
                return Ok(J::String(String::new()));
            }
            Ok(J::String(chars[begin..end].iter().collect()))
        }
        FnKind::Trim => {
            if vs.len() != 1 && vs.len() != 2 {
                return Err(arg_err());
            }
            let (chars, s) = if vs.len() == 1 {
                (None, &vs[0])
            } else {
                let J::String(c) = &vs[0] else {
                    return Err(arg_err());
                };
                (Some(c.as_str()), &vs[1])
            };
            let J::String(s) = s else {
                return Ok(J::Null);
            };
            let t = match chars {
                None => s.trim().to_string(),
                Some(cs) => s.trim_matches(|c| cs.contains(c)).to_string(),
            };
            Ok(J::String(t))
        }
        FnKind::Lower => match &vs[0] {
            J::String(s) => Ok(J::String(s.to_lowercase())),
            _ => Ok(J::Null),
        },
        FnKind::Upper => match &vs[0] {
            J::String(s) => Ok(J::String(s.to_uppercase())),
            _ => Ok(J::Null),
        },
        FnKind::CharLength => match &vs[0] {
            J::String(s) => Ok(J::from(s.chars().count() as i64)),
            _ => Ok(J::Null),
        },
        FnKind::Abs => Ok(J::from(num_of(&vs[0])?.abs())),
        FnKind::Ceil => Ok(J::from(num_of(&vs[0])?.ceil() as i64)),
        FnKind::Floor => Ok(J::from(num_of(&vs[0])?.floor() as i64)),
        FnKind::Round => {
            let x = num_of(&vs[0])?;
            let d = if vs.len() > 1 {
                num_of(&vs[1])? as i32
            } else {
                0
            };
            let m = 10f64.powi(d);
            Ok(number_from_f64((x * m).round() / m))
        }
    }
}

/// LIKE 模式长度上限（防病态回溯；P1-4）。
const MAX_LIKE_PATTERN: usize = 4096;

/// SQL LIKE：`%` 任意串、`_` 单字符（无转义符）。贪心 + 星号回溯，O(1) 额外内存。
pub fn like_match(s: &str, pat: &str) -> bool {
    let s: Vec<char> = s.chars().collect();
    let p: Vec<char> = pat.chars().collect();
    let (mut i, mut j) = (0usize, 0usize);
    let mut star: Option<usize> = None;
    let mut mark = 0usize;
    while i < s.len() {
        if j < p.len() && (p[j] == '_' || p[j] == s[i]) {
            i += 1;
            j += 1;
        } else if j < p.len() && p[j] == '%' {
            star = Some(j);
            mark = i;
            j += 1;
        } else if let Some(st) = star {
            j = st + 1;
            mark += 1;
            i = mark;
        } else {
            return false;
        }
    }
    while j < p.len() && p[j] == '%' {
        j += 1;
    }
    j == p.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---------- SQL 白名单 ----------

    #[test]
    fn parse_valid_matrix() {
        for expr in [
            "SELECT * FROM S3Object",
            "SELECT * FROM S3Object s",
            "SELECT s.name, s.age FROM S3Object s WHERE s.age > 30",
            "SELECT s._1, s._2 FROM S3Object",
            "SELECT * FROM S3Object WHERE s._1 = 'x' AND s._2 < 5",
            "SELECT * FROM S3Object LIMIT 10",
            "SELECT count(*) FROM S3Object",
            "SELECT sum(s.amount) FROM S3Object s WHERE s.region = 'eu'",
            "SELECT upper(s.name) AS up FROM S3Object s",
            "SELECT CAST(s.n AS INT) AS n2 FROM S3Object s",
            "SELECT s.a.b FROM S3Object s",
            "SELECT * FROM S3Object WHERE s.name LIKE '%jo%'",
            "SELECT * FROM S3Object WHERE s.n BETWEEN 1 AND 9",
            "SELECT * FROM S3Object WHERE s.t IN ('a','b')",
            "SELECT * FROM S3Object WHERE s.x IS NOT NULL",
        ] {
            parse(expr).unwrap_or_else(|e| panic!("应合法：{expr} → {e}"));
        }
    }

    #[test]
    fn parse_reject_matrix() {
        // 越白名单一律 400（不静默错结果）
        for expr in [
            "SELECT name FROM S3Object", // 缺 s. 前缀
            "SELECT * FROM Bucket",      // 非 S3Object
            "SELECT * FROM S3Object a",  // 别名非 s
            "SELECT DISTINCT s._1 FROM S3Object",
            "SELECT s._1 FROM S3Object GROUP BY s._1",
            "SELECT s._1 FROM S3Object ORDER BY s._1",
            "SELECT s._1 FROM S3Object OFFSET 1",
            "SELECT s._1 FROM S3Object LIMIT x",
            "SELECT count(s._1) FROM S3Object", // count 只收 *
            "SELECT sum(count(*)) FROM S3Object",
            "SELECT s._1, sum(s._2) FROM S3Object", // 聚合混排
            "SELECT s._1 FROM S3Object WHERE count(*) > 1",
            "SELECT s._1 FROM S3Object s JOIN t ON 1=1",
            "SELECT foo(s._1) FROM S3Object",          // 未知函数
            "SELECT CAST(s._1 AS DATE) FROM S3Object", // 不支持目标类型
            "SELECT s._1 FROM S3Object WHERE s._1 - 1 > 0", // 不支持的算术
            "INSERT INTO S3Object VALUES (1)",
            "SELECT * FROM S3Object; SELECT * FROM S3Object", // 多语句
            "SELECT * FROM S3Object WHERE s._0 = 1",          // CSV 0 基
            // 修复轮：白名单修饰符不得被静默丢弃
            "SELECT sum(DISTINCT s._2) FROM S3Object",
            "SELECT count(*) OVER () FROM S3Object",
            "SELECT lower(s._1) OVER () FROM S3Object",
            "SELECT count(*) FROM S3Object WINDOW w AS ()",
            "WITH x AS (SELECT 1) SELECT * FROM S3Object",
            "SELECT * FROM S3Object(1)",
            "SELECT s._1 FROM S3Object s(a, b)",
            "SELECT s._1 FROM S3Object WHERE s._1 LIKE 'a%' ESCAPE '\\'",
            "SELECT *, s._1 FROM S3Object",
            "SELECT count(*) = 1 FROM S3Object",
            "SELECT CAST(count(*) AS INT) FROM S3Object",
            // 参数个数（解析期拒绝，与数据无关）
            "SELECT lower() FROM S3Object",
            "SELECT abs() FROM S3Object",
            "SELECT round(s._1, 0, 2) FROM S3Object",
            "SELECT lower(s._1, 'x') FROM S3Object",
        ] {
            assert!(parse(expr).is_err(), "应拒绝：{expr}");
        }
    }

    // ---------- CSV 解析 ----------

    fn csv_opts() -> CsvIn {
        CsvIn::default()
    }

    #[test]
    fn csv_parse_quotes_and_escapes() {
        // 引号内含分隔符 / 双写转义引号 / 引号内换行（需 AllowQuotedRecordDelimiter=true）
        let mut o = csv_opts();
        o.allow_quoted_record_delimiter = true;
        let input = b"a,\"b,1\",\"c\"\"q\",\"multi\nline\"\nplain,2,3,4\n";
        let p = parse_csv(input, &o).unwrap();
        assert_eq!(p.records.len(), 2);
        assert_eq!(
            p.records[0].0,
            vec![
                "a".to_string(),
                "b,1".into(),
                "c\"q".into(),
                "multi\nline".into()
            ]
        );
        assert_eq!(p.records[1].0, vec!["plain", "2", "3", "4"]);
    }

    #[test]
    fn csv_parse_comment_and_header_and_custom_delim() {
        let mut o = csv_opts();
        o.comments = Some(b'#');
        o.header = HeaderMode::Use;
        o.field = b';';
        let p = parse_csv(b"# skip me\nname;age\nbob;42\n#skip2\nann;7\n", &o).unwrap();
        assert_eq!(p.header.unwrap(), vec!["name".to_string(), "age".into()]);
        assert_eq!(p.records.len(), 2);
        assert_eq!(p.records[1].0, vec!["ann", "7"]);
    }

    #[test]
    fn like_matrix() {
        assert!(like_match("hello", "h%"));
        assert!(like_match("hello", "%ll%"));
        assert!(like_match("hello", "h_llo"));
        assert!(like_match("hello", "hello"));
        assert!(!like_match("hello", "h_ll"));
        assert!(!like_match("hello", "x%"));
        assert!(like_match("", "%"));
    }

    // ---------- 求值 ----------

    fn csv_eval(sql: &str, data: &str, progress: bool) -> Vec<SelectEvent> {
        let plan = parse(sql).unwrap();
        evaluate(
            &plan,
            data.as_bytes(),
            &InFormat::Csv(csv_opts()),
            &OutFormat::Csv(CsvOut::default()),
            &EvalOptions {
                request_progress: progress,
                scanned: None,
                scan: None,
            },
        )
        .unwrap()
    }

    fn records_text(events: &[SelectEvent]) -> String {
        let mut s = Vec::new();
        for e in events {
            if let SelectEvent::Records(b) = e {
                s.extend_from_slice(b);
            }
        }
        String::from_utf8(s).unwrap()
    }

    #[test]
    fn eval_filter_projection_limit() {
        let data = "a,10\na,20\nb,30\n";
        assert_eq!(
            records_text(&csv_eval(
                "SELECT s._1 FROM S3Object WHERE s._2 > 15",
                data,
                false
            )),
            "a\nb\n"
        );
        assert_eq!(
            records_text(&csv_eval(
                "SELECT s._1, s._2 FROM S3Object LIMIT 1",
                data,
                false
            )),
            "a,10\n"
        );
        assert_eq!(
            records_text(&csv_eval("SELECT * FROM S3Object", data, false)),
            "a,10\na,20\nb,30\n"
        );
        // LIMIT 0：零输出（含聚合行）
        assert_eq!(
            records_text(&csv_eval("SELECT * FROM S3Object LIMIT 0", data, false)),
            ""
        );
        assert_eq!(
            records_text(&csv_eval(
                "SELECT count(*) FROM S3Object LIMIT 0",
                data,
                false
            )),
            ""
        );
    }

    #[test]
    fn evaluate_capped_enforces_record_and_byte_limits() {
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let opts = EvalOptions {
            request_progress: false,
            scanned: None,
            scan: None,
        };
        let eval = |max_records, max_bytes| {
            evaluate_capped(
                &plan,
                b"a\nb\nc\n",
                &InFormat::Csv(csv_opts()),
                &OutFormat::Csv(CsvOut::default()),
                &opts,
                max_records,
                max_bytes,
            )
        };
        // 3 条恰好到限 → 成功；再多一条则 400
        assert!(eval(3, 1024).is_ok());
        let err = eval(2, 1024).unwrap_err();
        assert!(err.message().unwrap().contains("记录上限"), "{err}");
        let err = eval(3, 3).unwrap_err();
        assert!(err.message().unwrap().contains("字节上限"), "{err}");
        // 上限内正常返回完整记录（3 条两字节 = 6 字节）
        assert_eq!(records_text(&eval(3, 6).unwrap()), "a\nb\nc\n");
    }

    #[test]
    fn eval_functions_and_null_semantics() {
        let data = "Alice,  x  ,3.7\nbob,,2.2\n";
        // upper/trim/substring/round/char_length
        let out = records_text(&csv_eval(
            "SELECT upper(s._1) AS u, trim(s._2) AS t, round(s._3) AS r, char_length(s._1) AS n FROM S3Object",
            data,
            false,
        ));
        assert_eq!(out, "ALICE,x,4,5\nBOB,,2,3\n");
        // NULL 语义：第二行空串不是 NULL；比较 NULL（JSON 缺字段）在 JSON 用例覆盖
        let out = records_text(&csv_eval(
            "SELECT substring(s._1, 2, 3) AS p FROM S3Object",
            data,
            false,
        ));
        assert_eq!(out, "lic\nob\n");
    }

    #[test]
    fn eval_like_between_in_cast() {
        let data = "apple,5,red\nbanana,12,yellow\ncherry,7,red\n";
        assert_eq!(
            records_text(&csv_eval(
                "SELECT s._1 FROM S3Object WHERE s._1 LIKE 'b%'",
                data,
                false
            )),
            "banana\n"
        );
        assert_eq!(
            records_text(&csv_eval(
                "SELECT s._1 FROM S3Object WHERE s._2 BETWEEN 5 AND 7",
                data,
                false
            )),
            "apple\ncherry\n"
        );
        assert_eq!(
            records_text(&csv_eval(
                "SELECT s._1 FROM S3Object WHERE s._3 IN ('red') AND s._2 > 6",
                data,
                false
            )),
            "cherry\n"
        );
        assert_eq!(
            records_text(&csv_eval(
                "SELECT CAST(s._2 AS FLOAT) AS f FROM S3Object LIMIT 1",
                data,
                false
            )),
            "5.0\n"
        );
    }

    #[test]
    fn eval_aggregates() {
        let data = "a,10\nb,20\nc,30\n";
        assert_eq!(
            records_text(&csv_eval("SELECT count(*) FROM S3Object", data, false)),
            "3\n"
        );
        assert_eq!(
            records_text(&csv_eval("SELECT sum(s._2) FROM S3Object", data, false)),
            "60\n"
        );
        assert_eq!(
            records_text(&csv_eval(
                "SELECT sum(s._2) FROM S3Object WHERE s._1 <> 'b'",
                data,
                false
            )),
            "40\n"
        );
        assert_eq!(
            records_text(&csv_eval("SELECT min(s._2) FROM S3Object", data, false)),
            "10\n"
        );
        assert_eq!(
            records_text(&csv_eval("SELECT avg(s._2) FROM S3Object", data, false)),
            "20\n"
        );
    }

    #[test]
    fn eval_json_paths_and_null() {
        let data = r#"{"name":"jo","a":{"b":7}}
{"name":"al","a":{"b":9}}"#;
        let plan = parse("SELECT s.name, s.a.b FROM S3Object s WHERE s.a.b > 8").unwrap();
        let events = evaluate(
            &plan,
            data.as_bytes(),
            &InFormat::Json { lines: true },
            &OutFormat::Json { record: b'\n' },
            &EvalOptions {
                request_progress: false,
                scanned: None,
                scan: None,
            },
        )
        .unwrap();
        let text = records_text(&events);
        let v: serde_json::Value = serde_json::from_str(text.trim()).unwrap();
        assert_eq!(v["name"], "al");
        assert_eq!(v["b"], 9, "无别名路径取末段作列名");
        // 缺字段 = NULL：IS NULL 命中
        let plan = parse("SELECT s.name FROM S3Object s WHERE s.zzz IS NULL").unwrap();
        let events = evaluate(
            &plan,
            data.as_bytes(),
            &InFormat::Json { lines: true },
            &OutFormat::Json { record: b'\n' },
            &EvalOptions {
                request_progress: false,
                scanned: None,
                scan: None,
            },
        )
        .unwrap();
        let lines: Vec<serde_json::Value> = records_text(&events)
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[1]["name"], "al");
    }

    #[test]
    fn stats_and_progress_rhythm() {
        // >1MiB 输入：Progress 恰好 floor(N/1MiB) 个（≥1），Stats 恒等式成立
        let mut data = String::with_capacity(1_300_000);
        let mut i = 0usize;
        while data.len() < 1_200_000 {
            data.push_str(&format!("k{i},v{i}\n"));
            i += 1;
        }
        let events = csv_eval("SELECT s._1 FROM S3Object", &data, true);
        let progress: Vec<&StatsData> = events
            .iter()
            .filter_map(|e| match e {
                SelectEvent::Progress(s) => Some(s),
                _ => None,
            })
            .collect();
        // 1.2MB 输入 → 1 帧，且首帧 offsets 钉在 1MiB（变异 PROGRESS_STEP=64MiB 会把 at 顶到 scanned）
        assert_eq!(progress.len(), 1, "1MiB+ 输入应恰好 1 帧 Progress");
        assert_eq!(progress[0].bytes_scanned, 1024 * 1024);
        // 单调递增 + 不超扫描总量
        for w in progress.windows(2) {
            assert!(w[0].bytes_scanned <= w[1].bytes_scanned);
        }
        let stats = events
            .iter()
            .rev()
            .find_map(|e| match e {
                SelectEvent::Stats(s) => Some(s),
                _ => None,
            })
            .unwrap();
        assert_eq!(stats.bytes_scanned, data.len() as i64);
        assert_eq!(stats.bytes_processed, data.len() as i64);
        assert!(stats.bytes_returned > 0);
        assert!(matches!(events.last(), Some(SelectEvent::End)));
        // 关闭进度 → 零 Progress
        let events = csv_eval("SELECT s._1 FROM S3Object", &data, false);
        assert!(
            events
                .iter()
                .all(|e| !matches!(e, SelectEvent::Progress(_))),
            "未启用时不得发 Progress"
        );
    }

    #[test]
    fn csv_output_quoting() {
        let data = "x,y\n";
        let plan = parse("SELECT s._1, s._2 FROM S3Object").unwrap();
        let c = CsvOut {
            always_quote: true,
            ..Default::default()
        };
        let events = evaluate(
            &plan,
            data.as_bytes(),
            &InFormat::Csv(csv_opts()),
            &OutFormat::Csv(c),
            &EvalOptions {
                request_progress: false,
                scanned: None,
                scan: None,
            },
        )
        .unwrap();
        assert_eq!(records_text(&events), "\"x\",\"y\"\n");
    }

    // ---------- 修复轮：3VL / 布尔强制 / ceil-floor / UTF-8 / LIKE / ScanRange ----------

    fn json_eval(sql: &str, data: &str) -> String {
        let plan = parse(sql).unwrap();
        let events = evaluate(
            &plan,
            data.as_bytes(),
            &InFormat::Json { lines: true },
            &OutFormat::Json { record: b'\n' },
            &EvalOptions {
                request_progress: false,
                scanned: None,
                scan: None,
            },
        )
        .unwrap();
        records_text(&events)
    }

    #[test]
    fn eval_three_valued_logic() {
        let data = "{\"n\":1}\n{\"m\":2}\n";
        // NOT UNKNOWN → UNKNOWN：两行都不满足 WHERE（AWS 返回 0 行）
        assert_eq!(
            json_eval("SELECT s.n FROM S3Object s WHERE NOT (s.n = 1)", data),
            ""
        );
        assert_eq!(
            json_eval("SELECT NOT s.nzzz AS x FROM S3Object s", data),
            "{\"x\":null}\n{\"x\":null}\n"
        );
        assert_eq!(
            json_eval(
                "SELECT (s.n = 1) AND (s.zzz = 2) AS x FROM S3Object s",
                data
            ),
            "{\"x\":null}\n{\"x\":null}\n"
        );
        assert_eq!(
            json_eval("SELECT 2 NOT IN (1, NULL) AS x FROM S3Object s", data),
            "{\"x\":null}\n{\"x\":null}\n"
        );
        // true OR Unknown = true；Unknown AND false = false
        assert_eq!(
            json_eval("SELECT (s.n = 1) OR (s.zzz = 2) AS x FROM S3Object s", data),
            "{\"x\":true}\n{\"x\":null}\n"
        );
        assert_eq!(
            json_eval(
                "SELECT (s.zzz = 2) AND (s.n = 1) AS x FROM S3Object s",
                data
            ),
            "{\"x\":null}\n{\"x\":null}\n"
        );
    }

    #[test]
    fn eval_non_boolean_where_rejected() {
        let data = "a,0\n,1\n";
        for sql in [
            "SELECT * FROM S3Object WHERE s._1",
            "SELECT * FROM S3Object WHERE 1",
            "SELECT * FROM S3Object WHERE s._1 AND s._2 = '1'",
        ] {
            let plan = parse(sql).unwrap();
            let r = evaluate(
                &plan,
                data.as_bytes(),
                &InFormat::Csv(csv_opts()),
                &OutFormat::Csv(CsvOut::default()),
                &EvalOptions {
                    request_progress: false,
                    scanned: None,
                    scan: None,
                },
            );
            assert!(r.is_err(), "应拒绝非布尔 WHERE：{sql}");
        }
    }

    #[test]
    fn eval_ceil_floor_and_function_null() {
        let data = "2.5\n-3.2\n";
        assert_eq!(
            records_text(&csv_eval(
                "SELECT ceil(s._1) AS c, floor(s._1) AS f FROM S3Object",
                data,
                false
            )),
            "3,2\n-3,-4\n"
        );
        // ceiling(NULL) → NULL（曾落成 0）
        assert_eq!(
            json_eval("SELECT ceiling(s.zzz) AS c FROM S3Object s", "{\"a\":1}"),
            "{\"c\":null}\n"
        );
    }

    #[test]
    fn csv_utf8_roundtrip() {
        let p = parse_csv("héllo,1\n".as_bytes(), &csv_opts()).unwrap();
        assert_eq!(p.records[0].0[0], "héllo");
        let p = parse_csv("a,\"hé,llo\",1\n".as_bytes(), &csv_opts()).unwrap();
        assert_eq!(p.records[0].0[1], "hé,llo");
        // 非法 UTF-8 → 400
        assert!(parse_csv(b"h\xff,1\n", &csv_opts()).is_err());
        // 输出：非 ASCII 按字节原样（引号分支不再逐字节 Latin-1 双重编码）
        let out = render_csv_values(&[J::String("hé,llo".into())], &CsvOut::default()).unwrap();
        assert_eq!(out, "\"hé,llo\"\n");
        let out = render_csv_values(&[J::String("héllo".into())], &CsvOut::default()).unwrap();
        assert_eq!(out, "héllo\n");
    }

    #[test]
    fn csv_output_escape_symmetric() {
        let cout = CsvOut {
            escape: b'\\',
            ..Default::default()
        };
        let out = render_csv_values(&[J::String("a\"b".into())], &cout).unwrap();
        assert_eq!(out, "\"a\\\"b\"\n");
        let cin = CsvIn {
            escape: b'\\',
            ..Default::default()
        };
        let p = parse_csv(out.as_bytes(), &cin).unwrap();
        assert_eq!(p.records[0].0[0], "a\"b");
        // 转义符自身也可回读
        let out = render_csv_values(&[J::String("x\\y".into())], &cout).unwrap();
        let p = parse_csv(out.as_bytes(), &cin).unwrap();
        assert_eq!(p.records[0].0[0], "x\\y");
    }

    #[test]
    fn csv_parse_default_comments_and_strict_quotes() {
        // AWS 缺省 comments = '#'
        let p = parse_csv(b"# comment\na,1\n", &CsvIn::default()).unwrap();
        assert_eq!(p.records.len(), 1);
        assert_eq!(p.records[0].0, vec!["a".to_string(), "1".into()]);
        // 缺省 AllowQuotedRecordDelimiter=false → 引号内换行 400；置 true 后放行
        let mut o = CsvIn::default();
        assert!(parse_csv(b"\"a\nb\",1\n", &o).is_err());
        o.allow_quoted_record_delimiter = true;
        assert_eq!(
            parse_csv(b"\"a\nb\",1\n", &o).unwrap().records[0].0[0],
            "a\nb"
        );
        // 未闭合引号 → 400
        assert!(parse_csv(b"\"oops,1\n", &CsvIn::default()).is_err());
    }

    #[test]
    fn like_backtracking_and_cap() {
        assert!(like_match("aaab", "%a%ab"));
        assert!(like_match("abcabc", "%bc%"));
        assert!(!like_match("ab", "a%c"));
        assert!(like_match("", "%"));
        assert!(like_match("x", "_"));
        // 模式 >4096 字节 → 400
        let sql = format!(
            "SELECT * FROM S3Object WHERE s._1 LIKE '{}'",
            "%".repeat(5000)
        );
        let plan = parse(&sql).unwrap();
        let r = evaluate(
            &plan,
            b"a,1\n",
            &InFormat::Csv(csv_opts()),
            &OutFormat::Csv(CsvOut::default()),
            &EvalOptions {
                request_progress: false,
                scanned: None,
                scan: None,
            },
        );
        assert!(r.is_err());
        // 自引用列模式（曾 O(n^2) 内存 OOM 的形态）按 O(1) 内存完成
        let data = format!("{}\n", "z".repeat(4096));
        let plan = parse("SELECT * FROM S3Object WHERE s._1 LIKE s._1").unwrap();
        let events = evaluate(
            &plan,
            data.as_bytes(),
            &InFormat::Csv(csv_opts()),
            &OutFormat::Csv(CsvOut::default()),
            &EvalOptions {
                request_progress: false,
                scanned: None,
                scan: None,
            },
        )
        .unwrap();
        assert!(records_text(&events).starts_with("zzz"));
    }

    #[test]
    fn eval_function_arity_rejected() {
        let data = "a,1\n";
        for sql in [
            "SELECT lower() FROM S3Object",
            "SELECT upper() FROM S3Object",
            "SELECT char_length() FROM S3Object",
            "SELECT abs() FROM S3Object",
            "SELECT ceil() FROM S3Object",
            "SELECT floor() FROM S3Object",
            "SELECT round() FROM S3Object",
            "SELECT round(s._1, 0, 2) FROM S3Object",
            "SELECT lower(s._1, 'x') FROM S3Object",
            "SELECT abs(s._1, s._1) FROM S3Object",
            "SELECT substring(NULL, 1, 2, 3) FROM S3Object",
        ] {
            // 允许 SQL 解析期或求值期拒绝；零参/多余参数不得进入求值或 panic
            if let Ok(plan) = parse(sql) {
                let r = evaluate(
                    &plan,
                    data.as_bytes(),
                    &InFormat::Csv(csv_opts()),
                    &OutFormat::Csv(CsvOut::default()),
                    &EvalOptions {
                        request_progress: false,
                        scanned: None,
                        scan: None,
                    },
                );
                assert!(r.is_err(), "应拒绝参数个数：{sql}");
            }
        }
    }

    #[test]
    fn eval_aggregate_alias_in_json() {
        let data = "1\n2\n";
        let run = |sql: &str| {
            let plan = parse(sql).unwrap();
            let events = evaluate(
                &plan,
                data.as_bytes(),
                &InFormat::Csv(csv_opts()),
                &OutFormat::Json { record: b'\n' },
                &EvalOptions {
                    request_progress: false,
                    scanned: None,
                    scan: None,
                },
            )
            .unwrap();
            records_text(&events)
        };
        assert_eq!(
            run("SELECT count(*) AS total FROM S3Object"),
            "{\"total\":2}\n"
        );
        assert_eq!(
            run("SELECT sum(s._1) AS total FROM S3Object"),
            "{\"total\":3}\n"
        );
        // 无别名 → _1
        assert_eq!(run("SELECT count(*) FROM S3Object"), "{\"_1\":2}\n");
    }

    #[test]
    fn eval_json_document_root_array() {
        let data = r#"[{"a":1},{"a":2}]"#;
        let plan = parse("SELECT s.a FROM S3Object s").unwrap();
        let events = evaluate(
            &plan,
            data.as_bytes(),
            &InFormat::Json { lines: false },
            &OutFormat::Csv(CsvOut::default()),
            &EvalOptions {
                request_progress: false,
                scanned: None,
                scan: None,
            },
        )
        .unwrap();
        assert_eq!(records_text(&events), "1\n2\n");
        // ScanRange 与 DOCUMENT 组合 → 明确 400
        let r = evaluate(
            &plan,
            data.as_bytes(),
            &InFormat::Json { lines: false },
            &OutFormat::Csv(CsvOut::default()),
            &EvalOptions {
                request_progress: false,
                scanned: None,
                scan: Some((0, 5)),
            },
        );
        assert!(r.is_err());
    }

    #[test]
    fn records_chunking_bounded_frames() {
        let mut data = String::new();
        for i in 0..2000 {
            data.push_str(&format!("{}{i}\n", "y".repeat(190)));
        }
        let events = csv_eval("SELECT s._1 FROM S3Object", &data, false);
        let chunks: Vec<&Bytes> = events
            .iter()
            .filter_map(|e| match e {
                SelectEvent::Records(b) => Some(b),
                _ => None,
            })
            .collect();
        assert!(chunks.len() >= 2, "输出应多帧（实得 {}）", chunks.len());
        assert!(chunks.iter().all(|c| c.len() <= RECORD_CHUNK));
        assert!(chunks.iter().map(|c| c.len()).sum::<usize>() > RECORD_CHUNK);
    }

    #[test]
    fn eval_scan_range_record_semantics() {
        let data = "a,1\nbbbb,2\nc,3\n"; // 记录首字节 0 / 4 / 11
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let run = |lo: i64, hi: i64| {
            let events = evaluate(
                &plan,
                data.as_bytes(),
                &InFormat::Csv(csv_opts()),
                &OutFormat::Csv(CsvOut::default()),
                &EvalOptions {
                    request_progress: false,
                    scanned: Some(hi - lo + 1),
                    scan: Some((lo, hi)),
                },
            )
            .unwrap();
            records_text(&events)
        };
        assert_eq!(run(0, 0), "a,1\n");
        assert_eq!(run(1, 10), "bbbb,2\n");
        // 记录首字节落在区间末尾 → 整条处理（越过 end 不被截断）
        assert_eq!(run(4, 5), "bbbb,2\n");
        assert_eq!(run(100, 200), "");
    }

    // ---------- Parquet 输入 ----------

    /// 三行 fixture：id 1..3（REQUIRED INT64）、name 可空、score DOUBLE、SNAPPY 压缩。
    fn parquet_fixture() -> Vec<u8> {
        use parquet::basic::Compression;
        use parquet::data_type::{ByteArray, ByteArrayType, DoubleType, Int64Type};
        use parquet::file::properties::WriterProperties;
        use parquet::file::writer::SerializedFileWriter;
        use parquet::schema::parser::parse_message_type;
        use std::sync::Arc;

        let schema = Arc::new(
            parse_message_type(
                "message s3.select {
                   REQUIRED INT64 id;
                   OPTIONAL BINARY name (UTF8);
                   REQUIRED DOUBLE score;
                 }",
            )
            .unwrap(),
        );
        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build(),
        );
        let mut w = SerializedFileWriter::new(Vec::new(), schema, props).unwrap();
        let mut rg = w.next_row_group().unwrap();

        let mut c = rg.next_column().unwrap().unwrap();
        c.typed::<Int64Type>()
            .write_batch(&[1, 2, 3], None, None)
            .unwrap();
        c.close().unwrap();

        let mut c = rg.next_column().unwrap().unwrap();
        let names = [ByteArray::from("alice"), ByteArray::from("carol")];
        c.typed::<ByteArrayType>()
            .write_batch(&names, Some(&[1, 0, 1]), None)
            .unwrap();
        c.close().unwrap();

        let mut c = rg.next_column().unwrap().unwrap();
        c.typed::<DoubleType>()
            .write_batch(&[1.5, 2.5, 3.5], None, None)
            .unwrap();
        c.close().unwrap();
        rg.close().unwrap();
        w.into_inner().unwrap()
    }

    fn parquet_eval(sql: &str, out: &OutFormat) -> Vec<SelectEvent> {
        let plan = parse(sql).unwrap();
        let data = parquet_fixture();
        evaluate(
            &plan,
            &data,
            &InFormat::Parquet,
            out,
            &EvalOptions {
                request_progress: false,
                scanned: None,
                scan: None,
            },
        )
        .unwrap()
    }

    #[test]
    fn parquet_select_json_rows_and_nulls() {
        let events = parquet_eval(
            "SELECT s.id, s.name FROM S3Object s WHERE s.id >= 2",
            &OutFormat::Json { record: b'\n' },
        );
        assert_eq!(
            records_text(&events),
            "{\"id\":2,\"name\":null}\n{\"id\":3,\"name\":\"carol\"}\n"
        );
    }

    #[test]
    fn parquet_select_csv_aggregate() {
        let events = parquet_eval(
            "SELECT sum(s.score) FROM S3Object s",
            &OutFormat::Csv(CsvOut::default()),
        );
        assert_eq!(records_text(&events), "7.5\n");
        // SELECT *：整行 JSON 对象直出（Parquet 行即 JSON 文档）
        let events = parquet_eval(
            "SELECT * FROM S3Object s WHERE s.id = 1",
            &OutFormat::Json { record: b'\n' },
        );
        assert_eq!(
            records_text(&events),
            "{\"id\":1,\"name\":\"alice\",\"score\":1.5}\n"
        );
    }

    #[test]
    fn parquet_rejects_scan_range_and_garbage() {
        let plan = parse("SELECT * FROM S3Object").unwrap();
        let data = parquet_fixture();
        let err = evaluate(
            &plan,
            &data,
            &InFormat::Parquet,
            &OutFormat::Csv(CsvOut::default()),
            &EvalOptions {
                request_progress: false,
                scanned: None,
                scan: Some((0, 10)),
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("ScanRange"), "{err}");
        let err = parse_parquet(b"not a parquet file").unwrap_err();
        assert!(err.to_string().contains("Parquet"), "{err}");
    }
}
