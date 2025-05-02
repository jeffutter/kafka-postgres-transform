// JavaScript transformation plugin

/**
 * Transform a message from Kafka before inserting into PostgreSQL
 * @param {Object} input - The input message from Kafka
 * @returns {Object} - The transformation result
 */
function transform(input) {
  try {
    // Handle customer data directly
    if (input.id && input.name) {
      console.log(`Processing customer ${input.name}`);
      
      return {
        success: true,
        data: {
          table_info: {
            name: "customers",
            schema: "public"
          },
          data: {
            customer_id: input.id,
            customer_name: input.name
          }
        }
      };
    }
    
    // Extract customer and order data if present
    if (input.customer && input.order) {
      // Extract customer data
      const customerId = input.customer.id;
      const customerName = input.customer.name;
      
      if (customerId === undefined) {
        return {
          success: false,
          error: "Missing customer id"
        };
      }
      
      if (customerName === undefined) {
        return {
          success: false,
          error: "Missing customer name"
        };
      }
      
      // Extract order data
      const orderId = input.order.id;
      const items = input.order.items;
      
      if (orderId === undefined) {
        return {
          success: false,
          error: "Missing order id"
        };
      }
      
      if (!Array.isArray(items)) {
        return {
          success: false,
          error: "Missing order items"
        };
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
      
      // Create transformed data
      return {
        success: true,
        data: {
          table_info: {
            name: "orders",
            schema: "public"
          },
          data: {
            order_id: orderId,
            customer_id: customerId,
            customer_name: customerName,
            total_items: totalItems,
            total_price: totalPrice
          }
        }
      };
    }
    
    // Simple pass-through for other data
    return {
      success: true,
      data: {
        table_info: {
          name: "generic",
          schema: "public"
        },
        data: input
      }
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
