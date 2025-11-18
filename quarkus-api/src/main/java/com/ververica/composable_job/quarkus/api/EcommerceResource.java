package com.ververica.composable_job.quarkus.api;

import com.ververica.composable_job.model.ProcessingEvent;
import com.ververica.composable_job.model.ecommerce.*;
import com.ververica.composable_job.quarkus.kafka.streams.ProductCacheService;
import com.ververica.composable_job.quarkus.persistence.OrderEntity;
import com.ververica.composable_job.quarkus.persistence.OrderItemEntity;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.logging.Log;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletionStage;

@Path("/api/ecommerce")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class EcommerceResource {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Map<String, ShoppingCart> carts = new ConcurrentHashMap<>();
    private static final Map<String, Order> orders = new ConcurrentHashMap<>();

    @Channel("ecommerce_events")
    Emitter<String> eventEmitter;

    @Channel("order_events")
    Emitter<String> orderEventEmitter;

    @Inject
    EcommerceEventService eventService;
    
    @Inject
    ProductCacheService productCacheService;

    @GET
    @Path("/products")
    public Response getProducts(@QueryParam("category") String category,
                               @QueryParam("search") String search,
                               @QueryParam("limit") @DefaultValue("20") int limit) {
        List<Product> products;
        
        // Use ProductCacheService which gets data from Kafka Streams cache (CQRS pattern)
        if (search != null && !search.isEmpty()) {
            products = productCacheService.searchProducts(search);
        } else if (category != null && !category.isEmpty()) {
            products = productCacheService.getProductsByCategory(category);
        } else {
            products = new ArrayList<>(productCacheService.getAllProducts());
        }
        
        if (search != null && !search.isEmpty()) {
            String searchLower = search.toLowerCase();
            products = products.stream()
                .filter(p -> p.name.toLowerCase().contains(searchLower) || 
                           p.description.toLowerCase().contains(searchLower))
                .toList();
        }
        
        return Response.ok(products.stream().limit(limit).toList()).build();
    }

    @GET
    @Path("/products/featured")
    public Response getFeaturedProducts() {
        List<Product> featured = productCacheService.getAllProducts().stream()
            .filter(p -> p.rating >= 4.0)
            .limit(8)
            .toList();
        return Response.ok(featured).build();
    }

    @GET
    @Path("/products/trending")
    public Response getTrendingProducts() {
        List<Product> trending = productCacheService.getAllProducts().stream()
            .sorted((a, b) -> Integer.compare(b.reviewCount, a.reviewCount))
            .limit(8)
            .toList();
        return Response.ok(trending).build();
    }

    @GET
    @Path("/inventory/state")
    public Response getInventoryState() {
        // Get all products with current inventory levels
        List<Product> allProducts = new ArrayList<>(productCacheService.getAllProducts());
        Map<String, Object> state = new HashMap<>();
        state.put("products", allProducts);
        state.put("totalProducts", allProducts.size());
        state.put("timestamp", System.currentTimeMillis());
        state.put("categories", allProducts.stream()
            .map(p -> p.category)
            .distinct()
            .toList());
        return Response.ok(state).build();
    }

    @GET
    @Path("/product/{id}")
    public Response getProduct(@PathParam("id") String productId) {
        Product product = productCacheService.getProduct(productId);
        if (product == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        return Response.ok(product).build();
    }

    @POST
    @Path("/events")
    public Response trackEvents(EventBatch batch) {
        try {
            for (Map<String, Object> eventData : batch.events) {
                EcommerceEvent event = MAPPER.convertValue(eventData, EcommerceEvent.class);
                
                // Send to Kafka
                ProcessingEvent<EcommerceEvent> processingEvent = ProcessingEvent.ofEcommerce(event);
                String eventJson = MAPPER.writeValueAsString(processingEvent);
                eventEmitter.send(eventJson);
                
                // Process event
                eventService.processEvent(event);
            }
            
            Log.infof("Processed %d events from session %s", batch.events.size(), batch.sessionId);
            return Response.ok().build();
        } catch (Exception e) {
            Log.error("Failed to process events", e);
            return Response.serverError().build();
        }
    }

    @GET
    @Path("/cart/{sessionId}")
    public Response getCart(@PathParam("sessionId") String sessionId) {
        ShoppingCart cart = carts.get(sessionId);
        if (cart == null) {
            cart = new ShoppingCart(UUID.randomUUID().toString(), sessionId, null, System.currentTimeMillis());
            carts.put(sessionId, cart);
        }
        return Response.ok(cart).build();
    }

    @POST
    @Path("/cart/{sessionId}/add")
    public Response addToCart(@PathParam("sessionId") String sessionId, CartAddRequest request) {
        Log.infof("--> ADD TO CART called for sessionId: %s", sessionId);
        
        ShoppingCart cart = carts.computeIfAbsent(sessionId, 
            k -> new ShoppingCart(UUID.randomUUID().toString(), sessionId, request.userId, System.currentTimeMillis()));
        
        Product product = productCacheService.getProduct(request.productId);
        if (product == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        
        CartItem item = new CartItem(product.productId, product.name, product.price, 
                                     request.quantity, product.imageUrl, System.currentTimeMillis());
        // cart.addItem(item);
        cart.upsertItem(item);

        Log.infof("  Cart updated. Current carts on server: %s", carts.keySet());
        
        // Send cart update event
        try {
            ProcessingEvent<ShoppingCart> cartEvent = ProcessingEvent.ofCart(cart);
            eventEmitter.send(MAPPER.writeValueAsString(cartEvent));
        } catch (Exception e) {
            Log.error("Failed to send cart event", e);
        }
        
        return Response.ok(cart).build();
    }

    @POST
    @Path("/checkout")
    @Transactional
    public Response checkout(CheckoutRequest request) {
        if (request == null || request.sessionId == null || request.sessionId.isBlank()) {
            Log.warn("❌ CHECKOUT FAILED: Received a checkout request with a null or empty sessionId.");
            return Response.status(Response.Status.BAD_REQUEST).entity("Session ID is missing.").build();
        }

        Log.infof("--> CHECKOUT called for sessionId: %s", request.sessionId);
        Log.infof("    Current carts on server: %s", carts.keySet());

        ShoppingCart cart = carts.get(request.sessionId);
        if (cart == null) {
            Log.warnf("❌ CHECKOUT FAILED: Cart object is NULL for sessionId: %s.", request.sessionId);
            return Response.status(Response.Status.BAD_REQUEST).entity("Cart not found.").build();
        }

        if (cart.items == null) {
            Log.warn("❌ CHECKOUT FAILED: cart.items collection is NULL.");
            return Response.status(Response.Status.BAD_REQUEST).entity("Cart items are missing.").build();
        }
        Log.infof("  Cart found with %d items.", cart.items.size());

        if (cart.items.isEmpty()) {
            Log.warn("❌ CHECKOUT FAILED: Cart is empty.");
            return Response.status(Response.Status.BAD_REQUEST)
                .entity("Cart is empty")
                .build();
        }

        // Create Order model for Kafka & response
        Order order = new Order(
            UUID.randomUUID().toString(),
            request.userId,
            request.sessionId,
            new ArrayList<>(cart.items),
            cart.totalAmount,
            cart.totalAmount * 0.08, // tax
            5.99, // shipping
            System.currentTimeMillis()
        );

        order.shippingAddress = request.shippingAddress;
        order.status = "CONFIRMED";

        // ========================================
        // PHASE 1: Persist to PostgreSQL for CDC
        // ========================================
        try {
            Log.info("  Attempting to persist order to PostgreSQL...");

            // Convert shipping address to JSON string
            String shippingAddressJson = null;
            if (order.shippingAddress != null) {
                shippingAddressJson = MAPPER.writeValueAsString(order.shippingAddress);
            }

            OrderEntity orderEntity = new OrderEntity(
                order.orderId,
                order.userId != null ? order.userId : "guest",
                Instant.ofEpochMilli(order.timestamp),
                order.status,
                BigDecimal.valueOf(order.subtotal),
                BigDecimal.valueOf(order.tax),
                BigDecimal.valueOf(order.shipping),
                BigDecimal.valueOf(order.totalAmount),
                request.paymentMethod,
                shippingAddressJson
            );

            // Persist order items
            for (CartItem item : cart.items) {
                OrderItemEntity itemEntity = new OrderItemEntity(
                    item.productId,
                    item.quantity,
                    BigDecimal.valueOf(item.price),
                    BigDecimal.ZERO,
                    BigDecimal.valueOf(item.price * item.quantity)
                );
                orderEntity.addItem(itemEntity);
            }

            // Persist to PostgreSQL (will trigger CDC)
            orderEntity.persist();

            Log.info("  ✅ Order " + order.orderId + " persisted to PostgreSQL successfully!");
            // Log.infof("✅ Order %s persisted to PostgreSQL (CDC will detect this)", order.orderId);

        } catch (Exception e) {
            Log.error("  ❌ CHECKOUT FAILED: Failed to persist order to PostgreSQL!", e);
            // Log.errorf("❌ Failed to persist order to PostgreSQL: %s", e.getMessage(), e);
            return Response.serverError().entity("Failed to process order").build();
        }

        // Store in memory for backward compatibility
        orders.put(order.orderId, order);
        carts.remove(request.sessionId); // Clear cart after order

        // Send order event to Kafka for immediate feedback (optional - CDC will also send)
        try {
            ProcessingEvent<Order> orderEvent = ProcessingEvent.ofOrder(order);
            // eventEmitter.send(MAPPER.writeValueAsString(orderEvent));
            // Wait for the eventEmitter send to complete
            CompletionStage<Void> ack = eventEmitter.send(MAPPER.writeValueAsString(orderEvent));
            ack.toCompletableFuture().join();
        } catch (Exception e) {
            Log.error("Failed to send order event", e);
        }

        // ========================================
        // Publish Order Items for Inventory Deduction
        //      Comment out if OrderCDCJob is used!
        // ========================================
        if (!request.useCdc) {
            try {
                Log.info("  Attempting to publish order items to Kafka...");

                for (CartItem item : cart.items) {
                    Map<String, Object> orderItemEvent = new HashMap<>();
                    orderItemEvent.put("orderId", order.orderId);
                    orderItemEvent.put("productId", item.productId);
                    orderItemEvent.put("quantity", item.quantity);
                    orderItemEvent.put("timestamp", System.currentTimeMillis());

                    String json = MAPPER.writeValueAsString(orderItemEvent);
                    // orderEventEmitter.send(json);
                    // Wait for the orderEventEmitter send to complete
                    CompletionStage<Void> ack = orderEventEmitter.send(json);
                    ack.toCompletableFuture().join();
                    Log.infof("    📦 SUCCESSFULLY PUBLISHED order item for inventory deduction: %s x%d", item.productId, item.quantity);
                }
            } catch (Exception e) {
                Log.error("  ❌ CHECKOUT FAILED: Failed to publish order items to Kafka!", e);
                // Log.error("Failed to publish order items", e);
            }
        } else {
            Log.info("  CDC mode enabled by frontend. Skipping direct Kafka publish.");
        }

        Log.info("✅ CHECKOUT ENDPOINT COMPLETED SUCCESSFULLY."); 
        return Response.ok(order).build();
    }

    @GET
    @Path("/recommendations/{userId}")
    public Response getRecommendations(@PathParam("userId") String userId) {
        // Generate mock recommendations from cached products
        List<String> productIds = productCacheService.getAllProducts().stream()
            .map(p -> p.productId)
            .limit(5)
            .toList();
        
        Recommendation recommendation = new Recommendation(
            UUID.randomUUID().toString(),
            userId,
            null,
            productIds,
            "PERSONALIZED",
            0.85,
            "Based on your browsing history",
            System.currentTimeMillis()
        );
        
        return Response.ok(recommendation).build();
    }

    // Product catalog is now managed by ProductCacheService using Kafka Streams (CQRS pattern)

    public static class EventBatch {
        public List<Map<String, Object>> events;
        public String sessionId;
        public String userId;
    }

    public static class CartAddRequest {
        public String productId;
        public String userId;
        public int quantity;
    }

    public static class CheckoutRequest {
        public String sessionId;
        public String userId;
        public Order.ShippingAddress shippingAddress;
        public String paymentMethod;
        // This field will capture the state of the 'Use CDC' checkbox from the frontend.
        public boolean useCdc; 
    }
}