use polars::prelude::*;
use wasm_bindgen::JsCast;

use crate::{
    conversion::Wrap, dataframe::JsDataFrame, error::JsPolarsErr, extern_iterator, extern_struct,
    JsResult,
};

use std::ops::Deref;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(js_name=Series)]
#[repr(transparent)]
pub struct JsSeries {
    pub(crate) series: Series,
}

impl JsSeries {
    pub(crate) fn new(series: Series) -> Self {
        JsSeries { series }
    }
}

impl From<Series> for JsSeries {
    fn from(series: Series) -> Self {
        Self { series }
    }
}

impl Deref for JsSeries {
    type Target = Series;

    fn deref(&self) -> &Self::Target {
        &self.series
    }
}

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(typescript_type = "Series")]
    pub type ExternSeries;

    #[wasm_bindgen(typescript_type = "any")]
    pub type ExternAnyValue;

    #[wasm_bindgen(method, getter = ptr)]
    fn ptr(this: &ExternSeries) -> f64;

    #[wasm_bindgen(static_method_of = ExternSeries)]
    fn wrap(ptr: u32) -> ExternSeries;

    #[wasm_bindgen(typescript_type = "Series[]")]
    pub type SeriesArray;

}

extern_struct!(ExternSeries, JsSeries);
extern_iterator!(SeriesArray, ExternSeries, JsSeries);

#[wasm_bindgen(js_class=Series)]
impl JsSeries {
    pub fn wrap(ptr: u32) -> JsSeries {
        unsafe { JsSeries::from_abi(ptr) }
    }
    pub fn new_str(name: &str, values: &js_sys::Array) -> JsResult<JsSeries> {
        let series =
            StringChunked::from_iter_options(name.into(), values.iter().map(|v| v.as_string()))
                .into_series();
        Ok(JsSeries { series })
    }
    pub fn new_bool(name: &str, values: &js_sys::Array) -> JsResult<JsSeries> {
        let series =
            BooleanChunked::from_iter_options(name.into(), values.iter().map(|v| v.as_bool()))
                .into_series();

        Ok(JsSeries { series })
    }
    pub fn new_f64(name: &str, values: &js_sys::Array) -> JsResult<JsSeries> {
        let series = Float64Chunked::from_iter_options(
            name.into(),
            values.iter().map(|v: JsValue| v.as_f64()),
        )
        .into_series();
        Ok(JsSeries { series })
    }
    pub fn new_i8(name: &str, values: &js_sys::Array) -> JsResult<JsSeries> {
        let series = Int8Chunked::from_iter_options(
            name.into(),
            values.iter().map(|v: JsValue| v.as_f64().map(|n| n as i8)),
        )
        .into_series();
        Ok(JsSeries { series })
    }
    pub fn new_series_list(name: &str, val: SeriesArray, _strict: bool) -> Self {
        let vals = val.into_iter().map(|x| x.series).collect::<Box<[Series]>>();
        Series::new(name.into(), &vals).into()
    }

    pub fn new_int_8_array(name: String, arr: &mut [i8]) -> JsSeries {
        Series::new(name.into(), arr).into()
    }

    pub fn new_uint_8_array(name: String, arr: &mut [u8]) -> JsSeries {
        Series::new(name.into(), arr).into()
    }

    pub fn new_int_16_array(name: String, arr: &mut [i16]) -> JsSeries {
        Series::new(name.into(), arr).into()
    }

    pub fn new_uint_16_array(name: String, arr: &mut [u16]) -> JsSeries {
        Series::new(name.into(), arr).into()
    }

    pub fn new_int_32_array(name: String, arr: &mut [i32]) -> JsSeries {
        Series::new(name.into(), arr).into()
    }

    pub fn new_uint_32_array(name: String, arr: &mut [u32]) -> JsSeries {
        Series::new(name.into(), arr).into()
    }

    pub fn new_int_64_array(name: String, arr: &mut [i64]) -> JsSeries {
        Series::new(name.into(), arr).into()
    }

    pub fn new_uint_64_array(name: String, arr: &mut [u64]) -> JsSeries {
        Series::new(name.into(), arr).into()
    }

    pub fn new_float_32_array(name: String, arr: &mut [f32]) -> JsSeries {
        Series::new(name.into(), arr).into()
    }

    pub fn new_float_64_array(name: String, arr: &mut [f64]) -> JsSeries {
        Series::new(name.into(), arr).into()
    }

    pub fn new_opt_str_array(name: String, arr: js_sys::Array) -> JsSeries {
        let s: Series = arr
            .iter()
            .map(|v| {
                if v.is_null() || v.is_undefined() {
                    None
                } else {
                    v.as_string()
                }
            })
            .collect();
        s.with_name(name.into()).into()
    }

    pub fn new_opt_bool_array(name: String, arr: js_sys::Array) -> JsSeries {
        let s: Series = arr.iter().map(|v| v.as_bool()).collect();
        s.with_name(name.into()).into()
    }

    pub fn get_idx(&self, idx: usize) -> JsResult<JsValue> {
        let av = self.series.get(idx).map_err(JsPolarsErr::from)?;
        Ok(Wrap(av).into())
    }

    #[wasm_bindgen(getter)]
    pub fn name(&self) -> String {
        self.series.name().to_owned().into_string()
    }

    pub fn to_string(&self) -> String {
        format!("{}", self.series)
    }
    pub fn estimated_size(&self) -> i64 {
        self.series.estimated_size() as i64
    }

    pub fn rechunk(&mut self, in_place: bool) -> Option<JsSeries> {
        let series = self.series.rechunk();
        if in_place {
            self.series = series;
            None
        } else {
            Some(series.into())
        }
    }

    pub fn bitand(&self, other: &JsSeries) -> JsResult<JsSeries> {
        let out = self
            .series
            .bitand(&other.series)
            .map_err(JsPolarsErr::from)?;
        Ok(out.into())
    }
    pub fn bitor(&self, other: &JsSeries) -> JsResult<JsSeries> {
        let out = self
            .series
            .bitor(&other.series)
            .map_err(JsPolarsErr::from)?;
        Ok(out.into())
    }
    pub fn bitxor(&self, other: &JsSeries) -> JsResult<JsSeries> {
        let out = self
            .series
            .bitxor(&other.series)
            .map_err(JsPolarsErr::from)?;
        Ok(out.into())
    }

    pub fn chunk_lengths(&self) -> Vec<u32> {
        self.series.chunk_lengths().map(|i| i as u32).collect()
    }

    pub fn rename(&mut self, name: String) {
        self.series.rename(name.into());
    }

    pub fn mean(&self) -> Option<f64> {
        match self.series.dtype() {
            DataType::Boolean => {
                let s = self.series.cast(&DataType::UInt8).unwrap();
                s.mean()
            }
            _ => self.series.mean(),
        }
    }

    pub fn n_chunks(&self) -> u32 {
        self.series.n_chunks() as u32
    }

    pub fn limit(&self, num_elements: f64) -> JsSeries {
        self.series.limit(num_elements as usize).into()
    }

    pub fn slice(&self, offset: i64, length: f64) -> JsSeries {
        self.series.slice(offset, length as usize).into()
    }

    pub fn append(&mut self, other: &JsSeries) -> JsResult<()> {
        self.series
            .append(&other.series)
            .map_err(JsPolarsErr::from)?;
        Ok(())
    }

    pub fn extend(&mut self, other: &JsSeries) -> JsResult<()> {
        self.series
            .extend(&other.series)
            .map_err(JsPolarsErr::from)?;
        Ok(())
    }
    pub fn filter(&self, filter: &JsSeries) -> JsResult<JsSeries> {
        let filter_series = &filter.series;
        if let Ok(ca) = filter_series.bool() {
            let series = self.series.filter(ca).map_err(JsPolarsErr::from)?;
            Ok(JsSeries { series })
        } else {
            let err = "Expected a boolean mask".to_string();
            Err(err.into())
        }
    }

    pub fn add(&self, other: &JsSeries) -> JsResult<JsSeries> {
        let s = (&self.series + &other.series).map_err(JsPolarsErr::from)?;
        Ok(s.into())
    }

    pub fn sub(&self, other: &JsSeries) -> JsResult<JsSeries> {
        let s = (&self.series - &other.series).map_err(JsPolarsErr::from)?;
        Ok(s.into())
    }

    pub fn mul(&self, other: &JsSeries) -> JsResult<JsSeries> {
        let s = (&self.series * &other.series).map_err(JsPolarsErr::from)?;
        Ok(s.into())
    }

    pub fn div(&self, other: &JsSeries) -> JsResult<JsSeries> {
        let s = (&self.series / &other.series).map_err(JsPolarsErr::from)?;
        Ok(s.into())
    }

    pub fn rem(&self, other: &JsSeries) -> JsResult<JsSeries> {
        let s = (&self.series % &other.series).map_err(JsPolarsErr::from)?;
        Ok(s.into())
    }

    pub fn head(&self, length: Option<i64>) -> JsSeries {
        (self.series.head(length.map(|l| l as usize))).into()
    }

    pub fn tail(&self, length: Option<i64>) -> JsSeries {
        (self.series.tail(length.map(|l| l as usize))).into()
    }

    pub fn sort(&self, reverse: Option<bool>) -> JsResult<JsSeries> {
        let reverse = reverse.unwrap_or(false);
        let s = self
            .series
            .sort(SortOptions::default().with_order_descending(reverse))
            .map_err(JsPolarsErr::from)?;
        Ok(s.into())
    }

    pub fn argsort(&self, reverse: bool, nulls_last: bool) -> JsSeries {
        self.series
            .arg_sort(SortOptions {
                descending: reverse,
                nulls_last,
                multithreaded: true,
                maintain_order: false,
            })
            .into_series()
            .into()
    }

    pub fn unique(&self) -> JsResult<JsSeries> {
        let unique = self.series.unique().map_err(JsPolarsErr::from)?;
        Ok(unique.into())
    }

    pub fn unique_stable(&self) -> JsResult<JsSeries> {
        let unique = self.series.unique_stable().map_err(JsPolarsErr::from)?;
        Ok(unique.into())
    }

    pub fn arg_unique(&self) -> JsResult<JsSeries> {
        let arg_unique = self.series.arg_unique().map_err(JsPolarsErr::from)?;
        Ok(arg_unique.into_series().into())
    }

    pub fn arg_min(&self) -> Option<i64> {
        self.series.arg_min().map(|v| v as i64)
    }

    pub fn arg_max(&self) -> Option<i64> {
        self.series.arg_max().map(|v| v as i64)
    }

    pub fn take(&self, indices: Vec<u32>) -> JsResult<JsSeries> {
        let indices = UInt32Chunked::from_vec("".into(), indices);

        let take = self.series.take(&indices).map_err(JsPolarsErr::from)?;
        Ok(JsSeries::new(take))
    }

    pub fn take_with_series(&self, indices: &JsSeries) -> JsResult<JsSeries> {
        let idx = indices.series.u32().map_err(JsPolarsErr::from)?;
        let take = self.series.take(idx).map_err(JsPolarsErr::from)?;
        Ok(JsSeries::new(take))
    }

    pub fn null_count(&self) -> JsResult<i64> {
        Ok(self.series.null_count() as i64)
    }

    pub fn is_null(&self) -> JsSeries {
        Self::new(self.series.is_null().into_series())
    }

    pub fn is_not_null(&self) -> JsSeries {
        Self::new(self.series.is_not_null().into_series())
    }

    pub fn is_not_nan(&self) -> JsResult<JsSeries> {
        let ca = self.series.is_not_nan().map_err(JsPolarsErr::from)?;
        Ok(ca.into_series().into())
    }

    pub fn is_nan(&self) -> JsResult<JsSeries> {
        let ca = self.series.is_nan().map_err(JsPolarsErr::from)?;
        Ok(ca.into_series().into())
    }

    pub fn is_finite(&self) -> JsResult<JsSeries> {
        let ca = self.series.is_finite().map_err(JsPolarsErr::from)?;
        Ok(ca.into_series().into())
    }

    pub fn is_infinite(&self) -> JsResult<JsSeries> {
        let ca = self.series.is_infinite().map_err(JsPolarsErr::from)?;
        Ok(ca.into_series().into())
    }

    pub fn sample_frac(
        &self,
        frac: f64,
        with_replacement: bool,
        shuffle: bool,
        seed: Option<u64>,
    ) -> JsResult<JsSeries> {
        // Safety:
        // Wrap is transparent.
        let s = self
            .series
            .sample_frac(frac, with_replacement, shuffle, seed)
            .map_err(JsPolarsErr::from)?;
        Ok(s.into())
    }

    pub fn explode(&self) -> JsResult<JsSeries> {
        let s = self.series.explode().map_err(JsPolarsErr::from)?;
        Ok(s.into())
    }

    pub fn eq(&self, rhs: &JsSeries) -> JsResult<JsSeries> {
        Ok(Self::new(
            self.series
                .equal(&rhs.series)
                .map_err(JsPolarsErr::from)?
                .into_series(),
        ))
    }

    pub fn neq(&self, rhs: &JsSeries) -> JsResult<JsSeries> {
        Ok(Self::new(
            self.series
                .not_equal(&rhs.series)
                .map_err(JsPolarsErr::from)?
                .into_series(),
        ))
    }

    pub fn gt(&self, rhs: &JsSeries) -> JsResult<JsSeries> {
        Ok(Self::new(
            self.series
                .gt(&rhs.series)
                .map_err(JsPolarsErr::from)?
                .into_series(),
        ))
    }

    pub fn gt_eq(&self, rhs: &JsSeries) -> JsResult<JsSeries> {
        Ok(Self::new(
            self.series
                .gt_eq(&rhs.series)
                .map_err(JsPolarsErr::from)?
                .into_series(),
        ))
    }

    pub fn lt(&self, rhs: &JsSeries) -> JsResult<JsSeries> {
        Ok(Self::new(
            self.series
                .lt(&rhs.series)
                .map_err(JsPolarsErr::from)?
                .into_series(),
        ))
    }

    pub fn lt_eq(&self, rhs: &JsSeries) -> JsResult<JsSeries> {
        Ok(Self::new(
            self.series
                .lt_eq(&rhs.series)
                .map_err(JsPolarsErr::from)?
                .into_series(),
        ))
    }

    pub fn _not(&self) -> JsResult<JsSeries> {
        let bool = self.series.bool().map_err(JsPolarsErr::from)?;
        Ok((!bool).into_series().into())
    }

    pub fn as_str(&self) -> JsResult<String> {
        Ok(format!("{:?}", self.series))
    }

    pub fn len(&self) -> i64 {
        self.series.len() as i64
    }

    pub fn to_physical(&self) -> JsSeries {
        let s = self.series.to_physical_repr().into_owned();
        s.into()
    }
    pub fn to_list(&self) -> JsValue {
        todo!()
    }
    pub fn median(&self) -> Option<f64> {
        match self.series.dtype() {
            DataType::Boolean => {
                let s = self.series.cast(&DataType::UInt8).unwrap();
                s.median()
            }
            _ => self.series.median(),
        }
    }
    pub fn as_single_ptr(&mut self) -> JsResult<usize> {
        let ptr = self.series.as_single_ptr().map_err(JsPolarsErr::from)?;
        Ok(ptr)
    }
    pub fn drop_nulls(&self) -> Self {
        self.series.drop_nulls().into()
    }
    pub fn fill_null(&self, strategy: &str) -> JsResult<JsSeries> {
        let strat = match strategy {
            // "backward" => FillNullStrategy::Backward,
            // "forward" => FillNullStrategy::Forward,
            "min" => FillNullStrategy::Min,
            "max" => FillNullStrategy::Max,
            "mean" => FillNullStrategy::Mean,
            "zero" => FillNullStrategy::Zero,
            "one" => FillNullStrategy::One,
            s => return Err(format!("Strategy {} not supported", s).into()),
        };
        let series = self.series.fill_null(strat).map_err(JsPolarsErr::from)?;
        Ok(JsSeries::new(series))
    }

    pub fn clone(&self) -> Self {
        JsSeries::new(self.series.clone())
    }
    pub fn shift(&self, periods: i64) -> Self {
        let s = self.series.shift(periods);
        JsSeries::new(s)
    }
    pub fn zip_with(&self, mask: &JsSeries, other: &JsSeries) -> JsResult<JsSeries> {
        let mask = mask.series.bool().map_err(JsPolarsErr::from)?;
        let s = self
            .series
            .zip_with(mask, &other.series)
            .map_err(JsPolarsErr::from)?;
        Ok(JsSeries::new(s))
    }

    pub fn arr_lengths(&self) -> JsResult<JsSeries> {
        let ca = self.series.list().map_err(JsPolarsErr::from)?;
        let s = ca.lst_lengths().into_series();
        Ok(JsSeries::new(s))
    }

    pub fn n_unique(&self) -> JsResult<usize> {
        let n = self.series.n_unique().map_err(JsPolarsErr::from)?;
        Ok(n)
    }

    pub fn is_first(&self) -> JsResult<JsSeries> {
        todo!()
        // let out = self
        //     .series
        //     .is_first()
        //     .map_err(JsPolarsErr::from)?
        //     .into_series();
        // Ok(out.into())
    }

    pub fn shrink_to_fit(&mut self) {
        self.series.shrink_to_fit();
    }

    pub fn dot(&self, _other: &JsSeries) -> Option<f64> {
        todo!()
        // self.series.dot(&other.series)
    }

    pub fn dtype(&self) -> String {
        let dt: crate::datatypes::JsDataType = self.series.dtype().into();
        dt.to_string()
    }
    pub fn inner_dtype(&self) -> Option<String> {
        self.series.dtype().inner_dtype().map(|dt| {
            let dt: crate::datatypes::JsDataType = dt.into();
            dt.to_string()
        })
    }
}

// pub fn reinterpret(s: &Series, signed: bool) -> polars::prelude::Result<Series> {
//     match (s.dtype(), signed) {
//         (DataType::UInt64, true) => {
//             let ca = s.u64().unwrap();
//             Ok(ca.reinterpret_signed().into_series())
//         }
//         (DataType::UInt64, false) => Ok(s.clone()),
//         (DataType::Int64, false) => {
//             let ca = s.i64().unwrap();
//             Ok(ca.reinterpret_unsigned().into_series())
//         }
//         (DataType::Int64, true) => Ok(s.clone()),
//         _ => Err(PolarsError::ComputeError(
//             "reinterpret is only allowed for 64bit integers dtype, use cast otherwise".into(),
//         )),
//     }
// }
pub(crate) fn to_series_collection(iter: js_sys::Iterator) -> Vec<Series> {
    let cols: Vec<Series> = iter
        .into_iter()
        .map(|jsv| {
            let jsv = jsv.unwrap();
            let key = JsValue::from_str("ptr");
            let ptr = js_sys::Reflect::get(&jsv, &key).unwrap();
            let n: f64 = js_sys::Number::unchecked_from_js(ptr).into();
            let ser: JsSeries = unsafe { JsSeries::from_abi(n as u32) };
            ser.series
        })
        .collect();
    cols
}

// pub(crate) fn to_jsseries_collection(s: Vec<Series>) -> Vec<u32> {
//     use wasm_bindgen::convert::IntoWasmAbi;
//     let s: Vec<u32> = s
//         .into_iter()
//         .map(move |series| {
//             let js_ser = JsSeries { series };

//             js_ser.into_abi()
//         })
//         .collect();

//     s
//     // todo!()
// }
