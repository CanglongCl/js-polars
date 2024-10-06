use polars::lazy::dsl::Expr;
use polars::prelude::*;
use wasm_bindgen::prelude::*;

use super::JsExpr;

#[wasm_bindgen(js_name=StringNameSpace)]
pub struct JsStringNameSpace {
    pub(crate) inner: Expr,
}

#[wasm_bindgen(js_class=StringNameSpace)]
impl JsStringNameSpace {
    pub fn contains_literal(&self, literal: &str) -> JsExpr {
        self.inner
            .clone()
            .str()
            .contains_literal(lit(literal))
            .into()
    }

    pub fn contains(&self, pat: &str) -> JsExpr {
        self.inner.clone().str().contains(lit(pat), false).into()
    }

    pub fn ends_with(&self, sub: &str) -> JsExpr {
        self.inner.clone().str().ends_with(lit(sub)).into()
    }

    pub fn starts_with(&self, sub: &str) -> JsExpr {
        self.inner.clone().str().starts_with(lit(sub)).into()
    }
    pub fn extract(&self, pat: &str, group_index: usize) -> JsExpr {
        self.inner
            .clone()
            .str()
            .extract(lit(pat), group_index)
            .into()
    }

    pub fn extract_all(&self, pat: &JsExpr) -> JsExpr {
        self.inner
            .clone()
            .str()
            .extract_all(pat.inner.clone())
            .into()
    }

    pub fn lengths(&self) -> JsExpr {
        let function = |s: Series| {
            let ca = s.str()?;
            Ok(Some(ca.str_len_chars().into_series()))
        };
        self.inner
            .clone()
            .map(function, GetOutput::from_type(DataType::UInt32))
            .with_fmt("str.lengths")
            .into()
    }
}
