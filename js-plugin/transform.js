// JavaScript transformation plugin

/**
 * Transform messages from Kafka before inserting into PostgreSQL
 * @param {Array} inputs - Array of input messages from Kafka
 * @returns {Object} - The transformation result with a list of transformed data
 */
function transform(inputs) {
  try {
    if (!Array.isArray(inputs)) {
      return {
        success: false,
        error: "Input must be an array"
      };
    }

    // Determine the table type based on the first valid input
    let tableInfo = null;
    const transformedData = [];
    let hasError = false;
    let errorMessage = "";

    for (const input of inputs) {
      try {
        // Handle customer data directly
        if (input.id && input.name) {
          if (!tableInfo) {
            tableInfo = {
              name: "customers",
              schema: "public",
              columns: [
                { name: "customer_id", type: "string" },
                { name: "customer_name", type: "string" }
              ]
            };
          }

          transformedData.push({
            customer_id: input.id,
            customer_name: input.name
          });
        }
        // Extract customer and order data if present
        else if (input.customer && input.order) {
          // Extract customer data
          const customerId = input.customer.id;
          const customerName = input.customer.name;

          if (customerId === undefined) {
            console.log("Missing customer id");
            continue;
          }

          if (customerName === undefined) {
            console.log("Missing customer name");
            continue;
          }

          // Extract order data
          const orderId = input.order.id;
          const items = input.order.items;

          if (orderId === undefined) {
            console.log("Missing order id");
            continue;
          }

          if (!Array.isArray(items)) {
            console.log("Missing order items");
            continue;
          }

          // Calculate totals
          let totalItems = 0;
          let totalPrice = 0.0;

          for (const item of items) {
            const quantity = item.quantity || 0;
            const price = item.price || 0.0;

            totalItems += quantity;
            totalPrice += price * quantity;
          }

          // Log using the Rust op (simplified to avoid recursion)
          console.log(`Processing order ${orderId} for customer ${customerName}`);

          if (!tableInfo) {
            tableInfo = {
              name: "orders",
              schema: "public",
              columns: [
                { name: "order_id", type: "string" },
                { name: "customer_id", type: "string" },
                { name: "customer_name", type: "string" },
                { name: "total_items", type: "integer" },
                { name: "total_price", type: "decimal" },
              ]
            };
          }

          transformedData.push({
            order_id: orderId,
            customer_id: customerId,
            customer_name: customerName,
            total_items: totalItems,
            total_price: totalPrice
          });
        }
        // Simple pass-through for other data
        else {
          if (!tableInfo) {
            tableInfo = {
              name: "generic",
              schema: "public",
              columns: []
            };
          }

          // transformedData.push(input);
          console.log(`Error processing item: ${itemError.message}`);
        }
      } catch (itemError) {
        console.log(`Error processing item: ${itemError.message}`);
        // Continue processing other items instead of failing the whole batch
      }
    }

    if (transformedData.length === 0) {
      return {
        success: false,
        error: "No valid data to transform"
      };
    }

    return {
      success: true,
      table_info: tableInfo,
      data: transformedData
    };
  } catch (error) {
    // Log the error using a simpler approach
    console.log(`Error in transform: ${error.message}`);

    return {
      success: false,
      error: `Error in transform: ${error.message}`
    };
  }
}

// Make the transform function available to the Deno runtime
globalThis.transform = transform;
