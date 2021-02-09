#[allow(unused_imports)]

use crate::ffi::*;
use crate::errors::*;

use std::ffi::CStr;
use std::ffi::CString;
use std::sync::Arc;
use std::ptr;
use std::os::raw::{c_char, c_int, c_longlong, c_ulong};

use serde::{Serialize, Deserialize};

use arrow::csv;

use arrow::array::Array;
use arrow::array::ArrayData;

use arrow::record_batch::RecordBatch;
use arrow::util::integration_util::*;
use arrow::ffi::{ArrowArray, FFI_ArrowArray, FFI_ArrowSchema};

#[allow(unused_imports)]
use arrow::datatypes::Field;
use arrow::datatypes::DataType;

use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef; // SchemaRef = Arc<Schema>

use datafusion::dataframe::DataFrame;

// use datafusion::logical_plan::DFSchemaRef;
// use datafusion::logical_plan::DFSchema;

use datafusion::execution::context::ExecutionContext;
use datafusion::physical_plan::csv::CsvReadOptions;



#[cfg(feature = "prettyprint")]
use arrow::util::pretty::print_batches;

#[allow(dead_code)]
#[allow(unused_variables)]

// #[test]
fn should_fail() {
    unimplemented!();
}

#[test]
fn should_pass() {
    eprint!("should_pass did pass");
}

#[no_mangle]
pub extern fn hello_arcolyte() {
    println!("Hello, arcolyte 2021!");
}

/// Add two signed integers.
///
/// On a 64-bit system, arguments are 32 bit and return type is 64 bit.
#[no_mangle]
pub extern fn add_numbers(x: c_int, y: c_int) -> c_longlong {
    x as c_longlong + y as c_longlong
}

/// Take a zero-terminated C string and return its length as a
/// machine-size integer.
#[no_mangle]
pub extern fn string_length(sz_msg: *const c_char) -> c_ulong {
    let slice = unsafe { CStr::from_ptr(sz_msg) };
    slice.to_bytes().len() as c_ulong
}



#[no_mangle]
pub extern fn test_schema_equality() {
}

#[allow(dead_code)]
#[allow(unused_variables)]
#[test]
pub extern fn demo_schema_equality() {
    let json = r#"
    {
        "fields": [
            {
                "name": "c1",
                "type": {"name": "int", "isSigned": true, "bitWidth": 32},
                "nullable": true,
                "children": []
            },
            {
                "name": "c2",
                "type": {"name": "floatingpoint", "precision": "DOUBLE"},
                "nullable": true,
                "children": []
            },
            {
                "name": "c3",
                "type": {"name": "utf8"},
                "nullable": true,
                "children": []
            },
            {
                "name": "c4",
                "type": {
                    "name": "list"
                },
                "nullable": true,
                "children": [
                    {
                        "name": "custom_item",
                        "type": {
                            "name": "int",
                            "isSigned": true,
                            "bitWidth": 32
                        },
                        "nullable": false,
                        "children": []
                    }
                ]
            }
        ]
    }"#;
    let json_schema: ArrowJsonSchema = serde_json::from_str(json).unwrap();
    let schema = Schema::new(vec![
        Field::new("c1", DataType::Int32, true),
        Field::new("c2", DataType::Float64, true),
        Field::new("c3", DataType::Utf8, true),
        Field::new(
            "c4",
            DataType::List(Box::new(Field::new(
                "custom_item",
                DataType::Int32,
                false,
            ))),
            true,
        ),
    ]);


    //assert!(json_schema.equals_schema(&schema));
}

/*
#[no_mangle]
pub extern fn json_to_arrow(json_name: &str, arrow_name: &str, verbose: bool) -> Result<()> {
    if verbose {
        eprintln!("Converting {} to {}", json_name, arrow_name);
    }

    let json_file = read_json_file(json_name)?;

    let arrow_file = File::create(arrow_name)?;
    let mut writer = FileWriter::try_new(arrow_file, &json_file.schema)?;

    for b in json_file.batches {
        writer.write(&b)?;
    }

    Ok(())
}
*/

#[test]
fn demo_arrow_ffi() {
    arrow_ffi()
}

#[no_mangle] pub extern fn arrow_ffi() {
    // let schema = FFI_ArrowSchema { format: null, name: null, metadata: null, flags: null, n_children: null, children: null, dictionary: null, release: null, private_data: null };
    // let array = FFI_ArrowArray { length: null, null_count: null, offset: null, n_buffers: null, n_children: null, buffers: null, children: null, dictionary: null, release: null, private_data: null };
}



/// Takes an input of "/tmp/arrow_to_json.json" and writes it to "/tmp/arrow_to_json.arrow"
#[test]
fn demo_json_to_arrow() {
    json_to_arrow()
}

#[no_mangle] pub extern fn json_to_arrow() {
    // TODO
    // arrow_to_json2("/tmp/arrow_to_json.arrow", "/tmp/arrow_to_json.json", true);
}


/// Takes an input of "/tmp/arrow_to_json.arrow" and writes it to "/tmp/arrow_to_json.json"
#[test]
fn demo_arrow_to_json2() {
    arrow_to_json();
}

#[no_mangle] 
pub extern fn arrow_to_json() {
    let _ = arrow_to_json2("/tmp/arrow_to_json.arrow", "/tmp/arrow_to_json.json", true);
}

fn load_arrow(file_name: &str) -> Result<Arc<Schema>> {
    let arrow_file = std::fs::File::open(file_name)?;

    eprintln!("Opened file…");

    let reader = arrow::ipc::reader::FileReader::try_new(arrow_file)?;
    eprintln!("Created Reader…");

    let rschema: Arc<Schema> = reader.schema();
    return Ok(rschema);
}

#[allow(dead_code)]
#[allow(unused_variables)]
fn arrow_to_json2(arrow_name: &str, json_name: &str, verbose: bool) -> Result<()> {
    if verbose {
        eprintln!("Converting {} to {}", arrow_name, json_name);
    }

    let arrow_file = std::fs::File::open(arrow_name)
    .chain_err(|| "The `_plugin_create` symbol wasn't found.")?;
    eprintln!("Opened file…");

    let reader = arrow::ipc::reader::FileReader::try_new(arrow_file)?;
    eprintln!("Created Reader…");

    let rschema: Arc<Schema> = reader.schema();

    let rschema = load_arrow(arrow_name).unwrap();

    let mut fields: Vec<ArrowJsonField> = vec![];
    for f in rschema.fields() {
        fields.push(ArrowJsonField::from(f));
    }
    
    eprintln!("Pushed fields…");
    let schema = ArrowJsonSchema { fields };

    eprintln!("Created schema…");

    let batches = reader
        .map(|batch| Ok(ArrowJsonBatch::from_batch(&batch?)))
        .collect::<Result<Vec<_>>>()?;

    let arrow_json = ArrowJson {
        schema,
        batches,
        dictionaries: None,
    };

    let json_file = std::fs::File::create(json_name)
    .chain_err(|| "The `_plugin_create` symbol wasn't found.")?;

    eprintln!("Created json_file…");

    serde_json::to_writer(&json_file, &arrow_json).unwrap();

    eprintln!("Wrote to json_file…");

    Ok(())
}


pub unsafe fn make_array_from_raw(array: *const FFI_ArrowArray, schema: *const FFI_ArrowSchema) -> Result<ArrowArray> {
    let array: ArrowArray = ArrowArray::try_from_raw(array, schema)?;
    // let data = Arc::new(ArrowArray::try_from(array)?);
    // Ok(arrow::array::make_array(data))
    // array.array.
    Ok(array)
}


// pub type AArray = FFI_ArrowArray;
// pub type ASchema = FFI_ArrowSchema;

#[no_mangle] pub unsafe extern 
fn arrow_ffi_test(array: *const FFI_ArrowArray, schema: *const FFI_ArrowSchema) {
    let _ = make_array_from_raw(array, schema);
}




#[derive(Serialize, Deserialize, Debug)]
#[repr(C)]
pub struct SerdePoint {
    x: i32,
    y: i32,
}

#[no_mangle]
pub extern fn serde_demo() -> SerdePoint {
    let point = SerdePoint { x: 1, y: 2 };

    // Convert the Point to a JSON string.
    let serialized = serde_json::to_string(&point).unwrap();

    // Prints serialized = {"x":1,"y":2}
    println!("serialized = {}", serialized);

    // Convert the JSON string back to a Point.
    let deserialized: SerdePoint = serde_json::from_str(&serialized).unwrap();

    // Prints deserialized = Point { x: 1, y: 2 }
    println!("deserialized = {:?}", deserialized);

    return deserialized;
}


#[no_mangle]
pub extern fn rust_hello(to: *const c_char) -> *mut c_char {
    let c_str = unsafe { CStr::from_ptr(to) };
    let recipient = match c_str.to_str() {
        Err(_) => "there",
        Ok(string) => string,
    };
    CString::new("Hello ".to_owned() + recipient).unwrap().into_raw()
}

#[no_mangle]
pub extern fn rust_hello_free(s: *mut c_char) {
    unsafe {
        if s.is_null() { return }
        CString::from_raw(s)
    };
}


#[no_mangle]
pub extern fn load_arrow_file(fname: *mut c_char) {
    let arrow: Result<Arc<Schema>> = unsafe { load_arrow(CStr::from_ptr(fname).to_str().unwrap()) };
    arrow.unwrap();
}


// #[repr(C)]
// pub struct ArrowSchemaVec {
//     vec: Vec<RecordBatch>,
// }


#[repr(C)]
pub struct ArrowVectorFFI {
    array: *const FFI_ArrowArray,
    schema: *const FFI_ArrowSchema,
}

#[allow(dead_code)]
#[allow(unused_variables)]
#[no_mangle]
pub extern fn arrow_array_ffi_roundtrip(arrow: *const ArrowVectorFFI) -> ArrowVectorFFI {
    let (array, schema) = unsafe { arrow_array_ffi_roundtrip_impl((*arrow).array, (*arrow).schema).unwrap() };
    return ArrowVectorFFI { array, schema };
}

fn arrow_array_ffi_roundtrip_impl(array: *const FFI_ArrowArray, schema: *const FFI_ArrowSchema) -> Result<(*const FFI_ArrowArray, *const FFI_ArrowSchema)> {
    // create a `ArrowArray` from the data.
    let d1: ArrowArray = unsafe { ArrowArray::try_from_raw(array, schema)? };

    // here we export the array as 2 pointers. We would have no control over ownership if it was not for
    // the release mechanism.
    let (array2, schema2) = ArrowArray::into_raw(d1);

    // simulate an external consumer by being the consumer
    // let d1 = unsafe { ArrowArray::try_from_raw(array2, schema2) }?;

    // let result = &ArrayData::try_from(d1);

    return Ok((array2, schema2));
}

#[allow(dead_code)]
#[allow(unused_variables)]
#[no_mangle]
pub extern fn arrow_array_ffi_arg_param_demo(buf: FFI_ArrowArray, param: i64) {
}


#[cfg(feature = "prettyprint")]
use arrow::util::pretty::print_batches;

use std::fs::File;


fn _arrow_load_csv(fname: &str) -> Result<()> {
    let file = File::open(fname).unwrap();
    let builder = csv::ReaderBuilder::new()
        .has_header(true)
        .infer_schema(Some(100));
    let mut csv = builder.build(file).unwrap();
    let _batch = csv.next().unwrap().unwrap();
    #[cfg(feature = "prettyprint")]
    {
        print_batches(&[_batch]).unwrap();
    }
    Ok(())
}


/// An Apache Arrow buffer
#[derive(Debug, Clone)]
pub struct ArrowFile {
    pub body: Option<Vec<u8>>,
}


#[no_mangle]
pub unsafe extern "C" fn arrow_load_csv(fname: *const c_char, rowcount: i64) -> *mut ArrowFile {
    if fname.is_null() {
        return error_ptr(Error::from("No file name provided"));
    }

    let raw = CStr::from_ptr(fname);

    let fname_as_str = match raw.to_str() {
        Ok(s) => s,
        Err(e) => return error_ptr(Error::with_chain(e, "Unable to convert URL to a UTF-8 string"))
    };

    let file = match File::open(fname_as_str) {
        Ok(u) => u,
        Err(e) => return error_ptr(Error::with_chain(e, "Unable to to open file"))
    };

    let builder = csv::ReaderBuilder::new()
        .has_header(true)
        .infer_schema(Some(100));
    let mut csv = builder.build(file).unwrap();

    let _batch = csv.next().unwrap().unwrap();

    if rowcount > 0 {
        #[cfg(feature = "prettyprint")]
        {
            print_batches(&[_batch]).unwrap(); // TODO: use rowcount
        }
    }

    return ptr::null_mut();
}

pub struct DataFrameState {
    /// Internal state for the context
    pub state: Arc<dyn DataFrame>,
}

#[no_mangle]
pub unsafe extern "C" fn datafusion_context_read_csv(ptr: *mut ExecutionContext, file_name: *const c_char) -> *mut DataFrameState {
    assert!(!ptr.is_null());
    let ctx = &mut *ptr;
    let df = match ctx.read_csv(&c2str(file_name), CsvReadOptions::new()) {
        Ok(s) => s,
        Err(e) => return error_ptr(Error::with_chain(e, "Unable to read CSV file"))
    };
    let dfs = DataFrameState { state: df };
    Box::into_raw(Box::new(dfs))
}

#[no_mangle]
pub unsafe extern "C" fn datafusion_context_read_parquet(ptr: *mut ExecutionContext, file_name: *const c_char) -> *mut DataFrameState {
    assert!(!ptr.is_null());
    let ctx = &mut *ptr;
    let df = match ctx.read_parquet(&c2str(file_name)) {
        Ok(s) => s,
        Err(e) => return error_ptr(Error::with_chain(e, "Unable to read Parquet file"))
    };

    let dfs = DataFrameState { state: df };
    Box::into_raw(Box::new(dfs))
}

/// Destroy a `DataFrame` once you are done with it.
#[no_mangle]
pub unsafe extern "C" fn datafusion_dataframe_destroy(ptr: *mut DataFrameState) {
    if !ptr.is_null() { Box::from_raw(ptr); }
}


/// E.g.: `"SELECT a, MIN(b) FROM example GROUP BY a LIMIT 100"`
#[no_mangle]
pub unsafe extern "C" fn datafusion_context_execute_sql(ptr: *mut ExecutionContext, sql: *const c_char) -> *mut DataFrameState {
    assert!(!ptr.is_null());
    let ctx = &mut *ptr;
    match ctx.sql(&c2str(sql)) {
        Ok(df) => Box::into_raw(Box::new(DataFrameState { state: df })),
        Err(e) => error_ptr(Error::with_chain(e, "Unable to execute SQL"))
    }
}

/// Applies the specified row limit to this data frame
#[no_mangle]
pub unsafe extern "C" fn datafusion_dataframe_limit(ptr: *mut DataFrameState, count: usize) -> *mut DataFrameState {
    assert!(!ptr.is_null());
    let df = &mut *ptr;
    match df.state.limit(count) {
        Ok(df2) => Box::into_raw(Box::new(DataFrameState { state: df2 })),
        Err(e) => error_ptr(Error::with_chain(e, "Unable to limit dataframe"))
    }
}

#[no_mangle]
pub unsafe extern "C" fn datafusion_dataframe_collect_count(ptr: *mut DataFrameState) -> usize {
    assert!(!ptr.is_null());
    let df = &mut *ptr;

    match tkrt().block_on(df.state.collect()) {
        Ok(x) => x.iter().map(|x| x.num_rows()).sum(),
        Err(e) => error_val(0, Error::with_chain(e, "Unable to collect DataFrame"))
    }
}

// async fn collect(&self) -> Result<Vec<RecordBatch>>;
// fn schema(&self) -> &DFSchema


// #[no_mangle]
// pub unsafe extern "C" fn datafusion_dataframe_schema(ptr: *mut DataFrameState) -> *mut DFSchemaRef {
//     assert!(!ptr.is_null());
//     let df: &DataFrameState = &mut *ptr;
//     let schema: &DFSchema = df.state.schema();

//     let xxx: DFSchema = *schema;
//     // let xxxx = xxx.to_dfschema_ref();

//     DFSchemaRef(xxx)
//     // *schema
//     // Box::into_raw(Box::new(schema))
//     // let schemaref = Arc::new(schema.to_dfschema_ref().unwrap());
//     // schemaref
// }

// #[macro_use]
use lazy_static::lazy_static;

thread_local!{
    // static LAST_ERROR: RefCell<Option<Box<dyn StdError>>> = RefCell::new(None);
    static TOKIO_RUNTIME: tokio::runtime::Runtime = tokio::runtime::Runtime::new().unwrap();
}

// lazy_static! {
//     static ref TOKIO_RUNTIME: tokio::runtime::Runtime = tokio::runtime::Runtime::new().unwrap();
// }

fn tkrt() -> tokio::runtime::Runtime {
    // TOKIO_RUNTIME.with(|x| {
    //     x
    // })

    // FOO.with(|f| {
    //     assert_eq!(*f.borrow(), 1);
    //     *f.borrow_mut() = 2;
    // });
    
    tokio::runtime::Runtime::new().unwrap()
}


/// Destroy an `ArrowArray` once you are done with it.
#[no_mangle]
pub unsafe extern "C" fn datafusion_arrow_destroy(ptr: *mut ArrowArray) {
    if !ptr.is_null() { Box::from_raw(ptr); }
}

// pub struct DataFrameState {
//     /// Internal state for the context
//     pub state: Arc<dyn DataFrame>,
// }

// pub struct ArrowArray {
//     // these are ref-counted because they can be shared by multiple buffers.
//     array: Arc<FFI_ArrowArray>,
//     schema: Arc<FFI_ArrowSchema>,
// }

pub type ExtArrowArray = ArrowArray;

#[no_mangle]
pub unsafe extern "C" fn datafusion_array_empty_create() -> *mut ExtArrowArray { 
    Box::into_raw(Box::new(ArrowArray::empty()))
}

// #[allow(dead_code)]
// #[allow(unused_variables)]
// #[no_mangle]
// pub unsafe extern "C" fn datafusion_dataframe_collect(ptr: *mut DataFrameState) -> ArrowSchemaVec {
//     assert!(!ptr.is_null());
//     let df = &mut *ptr;

//     let vec: Vec<RecordBatch> = tkrt().block_on(df.state.collect()).unwrap();
//     ArrowSchemaVec { vec }
// }

#[allow(dead_code)]
#[allow(unused_variables)]
#[no_mangle]
pub unsafe extern "C" fn datafusion_dataframe_collect_vector(ptr: *mut DataFrameState, index: usize) -> ArrowVectorFFI {
    assert!(!ptr.is_null());
    let df = &mut *ptr;

    let vec: Vec<RecordBatch> = tkrt().block_on(df.state.collect()).unwrap();

    let first: &RecordBatch = &vec[index];
    
    // pub struct RecordBatch {
    //     schema: SchemaRef,
    //     columns: Vec<Arc<Array>>,
    // }

    let schema: SchemaRef = first.schema();
    let columns: &[Arc<(dyn Array + 'static)>] = first.columns();

    let col_count: usize = first.num_columns();
    let row_count: usize = first.num_rows();

    let column: &Arc<(dyn Array + 'static)> = first.column(index);

    let array: &dyn Array = column.as_ref();

    let array_data: Arc<ArrayData> = column.data();
    let data_data_ref: &Arc<ArrayData> = column.data_ref();
    let data_data_ref2: &ArrayData = array_data.as_ref();
    let data_type: &DataType = column.data_type();
    
    let is_empty = array.is_empty();
    let len: usize = array.len();

    // convery the array to the C structs
    let raw: (*const FFI_ArrowArray, *const FFI_ArrowSchema) = array.to_raw().unwrap();
    let array = raw.0;
    let schema = raw.1;
        
    let asa = ArrowVectorFFI { array, schema };

    asa
    // return asa;

    // and re-wrap them in Arc
    // let array: ArrowArray = ArrowArray::try_from_raw(raw.0, raw.1).unwrap();

    // Box::into_raw(Box::new(array))
}

#[allow(dead_code)]
#[allow(unused_variables)]
#[no_mangle]
pub unsafe extern "C" fn datafusion_array_array_get(array: ArrowArray) -> *const FFI_ArrowArray {
    ArrowArray::into_raw(array).0
}

#[allow(dead_code)]
#[allow(unused_variables)]
#[no_mangle]
pub unsafe extern "C" fn datafusion_array_schema_get(array: ArrowArray) -> *const FFI_ArrowSchema {
    ArrowArray::into_raw(array).1

    // assert!(!ptr.is_null());

    // let carray: *const ArrowArray = ptr;
    // let array: &*const ArrowArray = &carray;

    // let xxx: Box<ArrowArray> = Box::from_raw(ptr);

    // let zzz = xxx.0;

    // let zzz = ArrowArray::into_raw(prt);

    // let r1: *const ArrowArray = carray as *const ArrowArray;

    // let r1 = carray as ArrowArray; // “an `as` expression can only be used to convert between primitive types or to coerce to a specific trait object”

    // let r2 = &mut num as *mut i32;

    // let raw: () = ptr.arrow_ffi();

    // let array: &mut ArrowArray = &mut *ptr;

    // let array2: ArrowArray = *ptr;

    // let b = *(ptr as *const ArrowArray);

    // let array3: &ArrowArray = &*ptr;

    // let array4: Box<ArrowArray> = Box::from_raw(ptr);

    // let xxx: *mut ArrowArray = Box::into_raw(array4);

    // let arrayref: &ArrowArray = array4.as_ref();

    // let arrayref2: &ArrowArray = array4.as_mut();

    // ptr.into_raw();

    // ArrowArray::into_raw();


    // let raw = array.into_raw();

    // let zzz: ArrowArray = *array3; // “cannot move out of `*array3` which is behind a shared reference”

    // let schema = array3.schema;

    // let xxx: *const FFI_ArrowSchema = ArrowArray::into_raw(carray).1; 

    // let xxx = ArrowArray::into_raw(ptr);
    
}


#[no_mangle]
pub extern "C" fn datafusion_context_create() -> *mut ExecutionContext {
    Box::into_raw(Box::new(ExecutionContext::new()))
}

/// Destroy an `ExecutionContext` once you are done with it.
#[no_mangle]
pub unsafe extern "C" fn datafusion_context_destroy(ptr: *mut ExecutionContext) {
    if !ptr.is_null() { Box::from_raw(ptr); }
}


/// A string we have faith in
unsafe fn c2str(chars: *const c_char) -> String {
    return CStr::from_ptr(chars).to_str().unwrap().to_string();
}

#[no_mangle]
pub unsafe extern "C" fn datafusion_context_register_csv(ptr: *mut ExecutionContext, file_name: *const c_char, table_name: *const c_char) {
    assert!(!ptr.is_null());
    let ctx = &mut *ptr;
    let _: *mut () = match ctx.register_csv(&c2str(table_name), &c2str(file_name), CsvReadOptions::new()) {
        Ok(_) => ptr::null_mut(),
        Err(e) => error_ptr(Error::with_chain(e, "Unable to register CSV file"))
    };
}

#[no_mangle]
pub unsafe extern "C" fn datafusion_context_register_parquet(ptr: *mut ExecutionContext, file_name: *const c_char, table_name: *const c_char) {
    assert!(!ptr.is_null());
    let ctx = &mut *ptr;
    let _: *mut () = match ctx.register_parquet(&c2str(table_name), &c2str(file_name)) {
        Ok(_) => ptr::null_mut(),
        Err(e) => error_ptr(Error::with_chain(e, "Unable to register Parquet file"))
    };
}


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_arrow_create_buffer() {
        eprintln!("Running test_arrow_create_buffer…");
        let buffer_u8 = arrow::buffer::Buffer::from(&[0u8, 1, 2, 3, 4, 5]);
        println!("{:?}", buffer_u8);
        eprintln!("…test_arrow_create_buffer");
    }

    #[test]
    fn test_datafusion_context_create() {
        eprintln!("Running datafusion_context_create…");
        let ctx = datafusion_context_create();
        let _ = unsafe { datafusion_context_register_csv(ctx, CString::new("test/data/csv/userdata1.csv").unwrap().into_raw(), CString::new("table1").unwrap().into_raw()) };
        unsafe { datafusion_context_destroy(ctx); }
        eprintln!("…datafusion_context_destroy");
    }

    /// Example from: https://github.com/apache/arrow/blob/master/rust/arrow/examples/read_csv.rs
    #[allow(dead_code)]
    #[allow(unused_variables)]
    #[test]
    pub fn test_arrow_csv() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("lat", DataType::Float64, false),
            Field::new("lng", DataType::Float64, false),
        ]);


        // be sure it exists, else:
        // thread '<unnamed>' panicked at 'called `Result::unwrap()` on an `Err` value: Os { code: 2, kind: NotFound, message: "No such file or directory" }', src/lib.rs:50:97
        //
        // let file = File::open("test/data/uk_cities.csv").unwrap();
        let file = std::fs::File::open("test/data/csv/uk_cities.csv").unwrap();

        let mut csv = csv::Reader::new(file, Arc::new(schema), false, None, 1024, None, None);
        let _batch = csv.next().unwrap().unwrap();

        let row_count = _batch.num_rows();

        #[cfg(feature = "prettyprint")]
        {
            print_batches(&[_batch]).unwrap();
        }

        Ok(())
        // return row_count;
    }
}
