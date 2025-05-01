use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Serialize, Deserialize)]
struct TransformResult {
    success: bool,
    data: Option<Value>,
    error: Option<String>,
}

#[no_mangle]
pub extern "C" fn transform(ptr: i32, len: i32) -> i32 {
    // Parse the input JSON
    let input_bytes = unsafe {
        let ptr = ptr as *const u8;
        std::slice::from_raw_parts(ptr, len as usize)
    };
    
    let input_str = match std::str::from_utf8(input_bytes) {
        Ok(s) => s,
        Err(_) => return return_error("Invalid UTF-8 in input"),
    };
    
    let input: Value = match serde_json::from_str(input_str) {
        Ok(v) => v,
        Err(_) => return return_error("Failed to parse input as JSON"),
    };
    
    // Simple transformation logic
    let result = match transform_data(&input) {
        Ok(data) => TransformResult {
            success: true,
            data: Some(data),
            error: None,
        },
        Err(e) => TransformResult {
            success: false,
            data: None,
            error: Some(e),
        },
    };
    
    // Serialize the result
    let result_json = match serde_json::to_string(&result) {
        Ok(s) => s,
        Err(_) => return return_error("Failed to serialize result"),
    };
    
    // Copy the result to memory that the host can access
    let result_bytes = result_json.as_bytes();
    let result_ptr = alloc(result_bytes.len() as i32);
    unsafe {
        let dest = result_ptr as *mut u8;
        std::ptr::copy_nonoverlapping(result_bytes.as_ptr(), dest, result_bytes.len());
    }
    
    result_ptr
}

fn transform_data(input: &Value) -> Result<Value, String> {
    // Extract customer and order data if present
    if let Some(customer) = input.get("customer") {
        if let Some(order) = input.get("order") {
            // Extract customer data
            let customer_id = customer.get("id")
                .and_then(|v| v.as_i64())
                .ok_or_else(|| "Missing customer id".to_string())?;
            
            let customer_name = customer.get("name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "Missing customer name".to_string())?;
            
            // Extract order data
            let order_id = order.get("id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "Missing order id".to_string())?;
            
            let items = order.get("items")
                .and_then(|v| v.as_array())
                .ok_or_else(|| "Missing order items".to_string())?;
            
            // Calculate totals
            let mut total_items = 0;
            let mut total_price = 0.0;
            
            for item in items {
                let quantity = item.get("quantity")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);
                
                let price = item.get("price")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0);
                
                total_items += quantity;
                total_price += price * quantity as f64;
            }
            
            // Create transformed data
            return Ok(json!({
                "table_info": {
                    "name": "orders",
                    "schema": "public"
                },
                "data": {
                    "order_id": order_id,
                    "customer_id": customer_id,
                    "customer_name": customer_name,
                    "total_items": total_items,
                    "total_price": total_price
                }
            }));
        }
    }
    
    // Simple pass-through for other data
    Ok(json!({
        "table_info": {
            "name": "generic",
            "schema": "public"
        },
        "data": input
    }))
}

fn return_error(message: &str) -> i32 {
    let result = TransformResult {
        success: false,
        data: None,
        error: Some(message.to_string()),
    };
    
    let result_json = match serde_json::to_string(&result) {
        Ok(s) => s,
        Err(_) => return 0, // Can't do much if we can't serialize the error
    };
    
    let result_bytes = result_json.as_bytes();
    let result_ptr = alloc(result_bytes.len() as i32);
    unsafe {
        let dest = result_ptr as *mut u8;
        std::ptr::copy_nonoverlapping(result_bytes.as_ptr(), dest, result_bytes.len());
    }
    
    result_ptr
}

// Memory management functions
#[no_mangle]
pub extern "C" fn alloc(size: i32) -> i32 {
    let mut buffer = Vec::<u8>::with_capacity(size as usize);
    let ptr = buffer.as_mut_ptr();
    std::mem::forget(buffer);
    ptr as i32
}

#[no_mangle]
pub extern "C" fn dealloc(ptr: i32, size: i32) {
    unsafe {
        let _ = Vec::from_raw_parts(ptr as *mut u8, 0, size as usize);
    }
}
