use super::{error::JsPolarsErr, JsResult};
use crate::dataframe::JsDataFrame;
use polars::prelude::*;
use std::io::Cursor;

use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub fn read_csv(
    buff: &[u8],
    separator: char,
    chunk_size: u32,
    has_header: bool,
    n_rows: Option<u32>,
    skip_rows: u32,
    rechunk: bool,
    encoding: String,
    n_threads: Option<u32>,
    low_memory: bool,
    parse_dates: bool,
    skip_rows_after_header: usize,
) -> JsResult<JsDataFrame> {
    let n_threads = n_threads.map(|i| i as usize);
    let n_rows = n_rows.map(|i| i as usize);
    let skip_rows = skip_rows as usize;
    let chunk_size = chunk_size as usize;

    let encoding = match encoding.as_ref() {
        "utf8" => CsvEncoding::Utf8,
        "utf8-lossy" => CsvEncoding::LossyUtf8,
        e => return Err(JsPolarsErr::Other(format!("encoding not {} not implemented.", e)).into()),
    };

    let df = CsvReadOptions::default()
        .with_has_header(has_header)
        .with_n_rows(n_rows)
        .with_parse_options(
            CsvParseOptions::default()
                .with_separator(separator as u8)
                .with_encoding(encoding)
                .with_try_parse_dates(parse_dates),
        )
        .with_skip_rows(skip_rows)
        .with_rechunk(rechunk)
        .with_chunk_size(chunk_size)
        .with_low_memory(low_memory)
        .with_n_threads(n_threads)
        .with_skip_rows_after_header(skip_rows_after_header)
        .into_reader_with_file_handle(Cursor::new(buff))
        .finish()
        .map_err(JsPolarsErr::from)?;

    Ok(df.into())
}
